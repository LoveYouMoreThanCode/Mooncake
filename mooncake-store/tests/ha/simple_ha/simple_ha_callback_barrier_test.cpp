#include "ha/oplog/ordered_oplog_writer.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake::test {
namespace {

using namespace std::chrono_literals;

class InMemorySink {
   public:
    ErrorCode Write(const OpLogBatchRecord& batch,
                    const DurablePrefix& /*expected_prefix*/) {
        std::lock_guard<std::mutex> lock(mutex_);
        batch_ids_.push_back(batch.batch_id);
        cv_.notify_all();
        return ErrorCode::OK;
    }

    bool WaitForBatches(size_t count, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout,
                            [&] { return batch_ids_.size() >= count; });
    }

   private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<uint64_t> batch_ids_;
};

OpLogEntry MakeEntry(const std::string& key = "key") {
    OpLogEntry entry;
    entry.op_type = OpType::PUT_END;
    entry.tenant_id = "default";
    entry.object_key = key;
    entry.payload = "value";
    return entry;
}

class OrderedOpLogWriterBarrierTest : public ::testing::Test {
   protected:
    void SetUp() override {
        writer_ = std::make_unique<OrderedOpLogWriter>(
            OrderedOpLogWriterConfig{},
            [this](const OpLogBatchRecord& batch,
                   const DurablePrefix& expected_prefix) {
                return sink_.Write(batch, expected_prefix);
            });
        writer_->Start();
    }

    void TearDown() override {
        ReleaseCallback();
        if (writer_) {
            writer_->Stop();
        }
    }

    std::unique_ptr<OrderedOpLogWriter::PendingHandle> Commit(
        OrderedOpLogWriter::DurableCallback callback = {}) {
        auto reservation = writer_->Reserve();
        if (!reservation) {
            return nullptr;
        }
        auto pending = writer_->Commit(std::move(*reservation), MakeEntry(),
                                       std::move(callback));
        if (!pending) {
            return nullptr;
        }
        return std::make_unique<OrderedOpLogWriter::PendingHandle>(
            std::move(*pending));
    }

    void BlockCallback() {
        std::unique_lock<std::mutex> lock(callback_mutex_);
        callback_entered_ = true;
        callback_cv_.notify_all();
        callback_cv_.wait(lock, [this] { return callback_released_; });
    }

    bool WaitForCallback() {
        std::unique_lock<std::mutex> lock(callback_mutex_);
        return callback_cv_.wait_for(lock, std::chrono::seconds(5),
                                     [this] { return callback_entered_; });
    }

    void ReleaseCallback() {
        {
            std::lock_guard<std::mutex> lock(callback_mutex_);
            callback_released_ = true;
        }
        callback_cv_.notify_all();
    }

    InMemorySink sink_;
    std::unique_ptr<OrderedOpLogWriter> writer_;

    std::mutex callback_mutex_;
    std::condition_variable callback_cv_;
    bool callback_entered_{false};
    bool callback_released_{false};
};

TEST(OrderedOpLogWriterBarrierStandaloneTest, CoversRestoredPrefix) {
    InMemorySink sink;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{
            .initial_durable_prefix = {.batch_id = 2, .last_seq = 10}},
        [&](const OpLogBatchRecord& batch, const DurablePrefix& prefix) {
            return sink.Write(batch, prefix);
        });

    EXPECT_TRUE(writer.WaitForCallbacksThrough(10, 0ms));
    // Not started, so a target beyond the restored prefix cannot progress.
    EXPECT_FALSE(writer.WaitForCallbacksThrough(11, 10ms));
}

TEST_F(OrderedOpLogWriterBarrierTest, AdvancesWithoutCallbacks) {
    for (int i = 0; i < 3; ++i) {
        ASSERT_NE(Commit(), nullptr);
    }
    EXPECT_TRUE(sink_.WaitForBatches(1, 5s));
    EXPECT_TRUE(writer_->WaitForCallbacksThrough(3, 5s));
}

TEST_F(OrderedOpLogWriterBarrierTest, WaitsForBlockedCallback) {
    auto pending =
        Commit([this](const OpLogEntry&) { BlockCallback(); });
    ASSERT_NE(pending, nullptr);
    ASSERT_TRUE(WaitForCallback());

    EXPECT_FALSE(writer_->WaitForCallbacksThrough(
        pending->sequence_id(), 50ms));

    ReleaseCallback();
    EXPECT_TRUE(
        writer_->WaitForCallbacksThrough(pending->sequence_id(), 5s));
}

TEST_F(OrderedOpLogWriterBarrierTest, StopWakesWaiter) {
    auto pending =
        Commit([this](const OpLogEntry&) { BlockCallback(); });
    ASSERT_NE(pending, nullptr);
    ASSERT_TRUE(WaitForCallback());

    std::thread stopper([this] { writer_->Stop(); });
    const auto started_at = std::chrono::steady_clock::now();
    EXPECT_FALSE(
        writer_->WaitForCallbacksThrough(pending->sequence_id(), 5s));
    EXPECT_LT(std::chrono::steady_clock::now() - started_at, 2s);

    ReleaseCallback();
    stopper.join();
}

}  // namespace
}  // namespace mooncake::test
