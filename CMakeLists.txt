cmake_minimum_required(VERSION 3.16)
project(KeyAwareSwitch LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(CTest)

set(KEYAWARE_CORE_SOURCES
    src/common/experiment_config.cpp
    src/common/config_loader.cpp
    src/common/he_params_loader.cpp
    src/model/workload.cpp
    src/search/tree_search.cpp
    src/backend/analytical_backend.cpp
    src/backend/primitive_simulator.cpp
    src/backend/cycle_backend.cpp
    src/backend/hybrid_backend.cpp
    src/backend/profile_table.cpp
    src/backend/table_backend.cpp
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
endif()
