cmake_minimum_required(VERSION 3.16)
project(MercuryCachePP VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ---------------------------------------------------------------------------
# Compiler hardening flags
# ---------------------------------------------------------------------------
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(
        -Wall -Wextra -Wpedantic
        -O2
        -fstack-protector-strong
        -D_FORTIFY_SOURCE=2
    )
endif()

# ---------------------------------------------------------------------------
# Dependencies (via CMake's find_package + pkg-config)
# ---------------------------------------------------------------------------
find_package(Threads REQUIRED)

# gRPC / Protobuf — installed via `apt install -y libgrpc++-dev protobuf-compiler-grpc`
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# ONNX Runtime (optional — enable with -DWITH_ONNX=ON)
option(WITH_ONNX "Enable ONNX Runtime for ML admission" OFF)
if(WITH_ONNX)
    find_package(onnxruntime REQUIRED)
    add_compile_definitions(MERCURY_WITH_ONNX=1)
endif()

# ---------------------------------------------------------------------------
# Protobuf / gRPC codegen
# ---------------------------------------------------------------------------
set(PROTO_SRC ${CMAKE_CURRENT_SOURCE_DIR}/grpc/cache_node.proto)
set(PROTO_OUT ${CMAKE_CURRENT_BINARY_DIR}/grpc)
file(MAKE_DIRECTORY ${PROTO_OUT})

# Generated files
set(PROTO_GEN_SRC
    ${PROTO_OUT}/cache_node.pb.cc
    ${PROTO_OUT}/cache_node.grpc.pb.cc
)

add_custom_command(
    OUTPUT ${PROTO_GEN_SRC}
           ${PROTO_OUT}/cache_node.pb.h
           ${PROTO_OUT}/cache_node.grpc.pb.h
    COMMAND protoc
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/grpc
        --cpp_out=${PROTO_OUT}
        --grpc_out=${PROTO_OUT}
        --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
        ${PROTO_SRC}
    DEPENDS ${PROTO_SRC}
    COMMENT "Generating protobuf and gRPC sources"
)

# ---------------------------------------------------------------------------
# Core library (header-only + CacheStore.cpp)
# ---------------------------------------------------------------------------
add_library(mercury_core STATIC
    cache_core/CacheStore.cpp
    ${PROTO_GEN_SRC}
    grpc/CacheNodeServer.cpp
)

target_include_directories(mercury_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}   # for generated proto headers
    PRIVATE
        ${PROTO_OUT}
)

target_link_libraries(mercury_core
    PUBLIC
        Threads::Threads
        protobuf::libprotobuf
        gRPC::grpc++
)

if(WITH_ONNX)
    target_link_libraries(mercury_core PUBLIC onnxruntime)
endif()

# ---------------------------------------------------------------------------
# Server executable
# ---------------------------------------------------------------------------
add_executable(cache_node_server
    main.cpp
)

target_link_libraries(cache_node_server PRIVATE mercury_core)

# ---------------------------------------------------------------------------
# Unit tests (Google Test)
# ---------------------------------------------------------------------------
option(BUILD_TESTS "Build unit tests" ON)
if(BUILD_TESTS)
    find_package(GTest QUIET)
    if(GTest_FOUND)
        enable_testing()

        add_executable(mercury_tests
            tests/test_cache_store.cpp
            tests/test_eviction.cpp
            tests/test_single_flight.cpp
            tests/test_hot_key_tracker.cpp
        )

        target_link_libraries(mercury_tests
            PRIVATE mercury_core GTest::GTest GTest::Main
        )

        include(GoogleTest)
        gtest_discover_tests(mercury_tests)
    else()
        message(WARNING "GTest not found — skipping unit tests")
    endif()
endif()
