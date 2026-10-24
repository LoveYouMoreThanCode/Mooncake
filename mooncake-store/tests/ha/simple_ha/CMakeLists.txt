# Simple HA test targets live here so the shared tests CMakeLists only needs
# one add_subdirectory() line.

# Core Simple HA logic that does not need the Transfer Engine. Links the
# master-only static library so these tests can also be built with WITH_TE=OFF.
function(add_ha_core_test name)
  add_executable(${name} ${ARGN})
  target_include_directories(${name} PRIVATE ${MOONCAKE_STORE_TESTS_SOURCE_DIR})
  target_link_libraries(
    ${name}
    PRIVATE mooncake_store_master
            cachelib_memory_allocator
            ${ETCD_WRAPPER_LIB}
            glog::glog
            gflags::gflags
            asio_shared
            yalantinglibs::yalantinglibs
            gtest
            gtest_main
            pthread)
  add_test(NAME ${name} COMMAND ${name})
endfunction()

add_ha_core_test(simple_ha_wal_test simple_ha_wal_test.cpp)
add_ha_core_test(simple_ha_callback_barrier_test
                 simple_ha_callback_barrier_test.cpp)
add_ha_core_test(simple_ha_restore_lease_test simple_ha_restore_lease_test.cpp)
add_ha_core_test(simple_ha_snapshot_cursor_test
                 simple_ha_snapshot_cursor_test.cpp)
add_ha_core_test(simple_ha_drain_test simple_ha_drain_test.cpp)
add_ha_core_test(simple_ha_l1_harness_test simple_ha_l1_harness_test.cpp)

# End-to-end supervisor test: spawns the real master binary and a local etcd.
if(STORE_USE_ETCD AND TARGET mooncake_master)
  add_executable(simple_ha_supervisor_e2e_test
                 simple_ha_supervisor_e2e_test.cpp
                 ../../../src/master_client.cpp
                 ../../../src/config/rpc_timeout_config.cpp)
  target_include_directories(
    simple_ha_supervisor_e2e_test
    PRIVATE ${MOONCAKE_STORE_TESTS_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/../../../src
            ${CMAKE_SOURCE_DIR}/mooncake-common/include
            ${CMAKE_SOURCE_DIR}/mooncake-common/src)
  target_compile_definitions(
    simple_ha_supervisor_e2e_test
    PRIVATE MOONCAKE_MASTER_BIN="$<TARGET_FILE:mooncake_master>")
  add_dependencies(simple_ha_supervisor_e2e_test mooncake_master)
  target_link_libraries(
    simple_ha_supervisor_e2e_test
    PRIVATE mooncake_store_master
            cachelib_memory_allocator
            ${ETCD_WRAPPER_LIB}
            glog::glog
            gflags::gflags
            yalantinglibs::yalantinglibs
            asio_shared
            gtest
            gtest_main
            pthread)
  add_test(NAME simple_ha_supervisor_e2e_test
           COMMAND simple_ha_supervisor_e2e_test)
  set_tests_properties(simple_ha_supervisor_e2e_test
                       PROPERTIES TIMEOUT 180)

  # Small CLI used by the shell smoke under this directory.
  add_executable(simple_ha_smoke_client
                 simple_ha_smoke_client.cpp
                 ../../../src/master_client.cpp
                 ../../../src/config/rpc_timeout_config.cpp)
  target_include_directories(
    simple_ha_smoke_client
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../../src
            ${CMAKE_SOURCE_DIR}/mooncake-common/include
            ${CMAKE_SOURCE_DIR}/mooncake-common/src)
  target_link_libraries(
    simple_ha_smoke_client
    PRIVATE mooncake_store_master
            cachelib_memory_allocator
            ${ETCD_WRAPPER_LIB}
            glog::glog
            gflags::gflags
            yalantinglibs::yalantinglibs
            asio_shared
            pthread)
endif()
