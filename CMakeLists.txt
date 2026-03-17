cmake_minimum_required(VERSION 3.16)
project(KeyAwareSwitch LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(KEYAWARE_ALWAYS_DUMP_LOGICAL_GRAPH
    "Always print the shared single-board logical graph at runtime"
    ON)
option(KEYAWARE_ALWAYS_DUMP_RUNTIME_PLAN
    "Always print the runtime-planned physical step graph at runtime"
    ON)

if(NOT DEFINED BUILD_TESTING)
    set(BUILD_TESTING OFF CACHE BOOL "Build tests" FORCE)
endif()
include(CTest)

set(KEYAWARE_CORE_SOURCES
    src/common/experiment_config.cpp
    src/common/config_loader.cpp
    src/common/he_params_loader.cpp
    src/model/workload.cpp
    src/backend/hw/hardware_model.cpp
    src/backend/cycle_sim/arch.cpp
    src/backend/cycle_sim/driver.cpp
    src/backend/cycle_sim/lowering.cpp
    src/backend/step_graph_runtime.cpp
    src/backend/runtime_planner.cpp
    src/backend/model/keyswitch_execution_model.cpp
    src/backend/primitive_simulator.cpp
    src/backend/cycle_backend.cpp
    src/scheduler/fifo_scheduler.cpp
    src/scheduler/affinity_scheduler.cpp
    src/scheduler/static_partition_scheduler.cpp
    src/scheduler/score_scheduler.cpp
    src/scheduler/pool_scheduler.cpp
    src/scheduler/hierarchical_scheduler.cpp
    src/sim/metrics.cpp
    src/sim/simulator.cpp
)

add_library(keyaware_core STATIC
    ${KEYAWARE_CORE_SOURCES}
)

target_include_directories(keyaware_core PUBLIC include)

if(KEYAWARE_ALWAYS_DUMP_LOGICAL_GRAPH)
    target_compile_definitions(keyaware_core PUBLIC
        KEYAWARE_ALWAYS_DUMP_LOGICAL_GRAPH=1)
endif()
if(KEYAWARE_ALWAYS_DUMP_RUNTIME_PLAN)
    target_compile_definitions(keyaware_core PUBLIC
        KEYAWARE_ALWAYS_DUMP_RUNTIME_PLAN=1)
endif()

target_compile_options(keyaware_core PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)

add_executable(keyaware_sim
    src/main.cpp
)

target_link_libraries(keyaware_sim PRIVATE keyaware_core)

target_compile_options(keyaware_sim PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)

if(BUILD_TESTING)
    add_executable(keyaware_cycle_tests
        tests/cycle_sim_tests.cpp
    )
    target_link_libraries(keyaware_cycle_tests PRIVATE keyaware_core)
    target_include_directories(keyaware_cycle_tests PRIVATE tests)
    target_compile_options(keyaware_cycle_tests PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )
    add_test(NAME keyaware_cycle_tests COMMAND keyaware_cycle_tests)

    add_executable(keyaware_runtime_plan_tests
        tests/runtime_plan_observability_tests.cpp
    )
    target_link_libraries(keyaware_runtime_plan_tests PRIVATE keyaware_core)
    target_include_directories(keyaware_runtime_plan_tests PRIVATE tests)
    target_compile_options(keyaware_runtime_plan_tests PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )
    add_test(NAME keyaware_runtime_plan_tests COMMAND keyaware_runtime_plan_tests)

    set(KEYAWARE_HAS_LEGACY_TEST_BACKENDS TRUE)
    if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/include/backend/analytical_backend.h")
        set(KEYAWARE_HAS_LEGACY_TEST_BACKENDS FALSE)
    endif()
    if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/include/backend/table_backend.h")
        set(KEYAWARE_HAS_LEGACY_TEST_BACKENDS FALSE)
    endif()

    if(KEYAWARE_HAS_LEGACY_TEST_BACKENDS)
        add_executable(keyaware_unit_tests
            tests/unit_tests.cpp
        )
        target_link_libraries(keyaware_unit_tests PRIVATE keyaware_core)
        target_include_directories(keyaware_unit_tests PRIVATE tests)
        target_compile_options(keyaware_unit_tests PRIVATE
            -Wall
            -Wextra
            -Wpedantic
        )

        add_executable(keyaware_regression_tests
            tests/regression_tests.cpp
        )
        target_link_libraries(keyaware_regression_tests PRIVATE keyaware_core)
        target_include_directories(keyaware_regression_tests PRIVATE tests)
        target_compile_options(keyaware_regression_tests PRIVATE
            -Wall
            -Wextra
            -Wpedantic
        )

        add_test(NAME keyaware_unit_tests COMMAND keyaware_unit_tests)
        add_test(NAME keyaware_regression_tests COMMAND keyaware_regression_tests)
    else()
        message(STATUS "Skipping legacy unit/regression tests: analytical/table backends are unavailable in this tree")
    endif()
endif()
