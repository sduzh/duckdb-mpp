cmake_minimum_required(VERSION 3.28)

# Set extension name here
set(TARGET_NAME mpp)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find Protobuf installation Looks for protobuf-config.cmake file installed by
# Protobuf's cmake installation.
option(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)

# Find gRPC installation Looks for gRPCConfig.cmake file installed by gRPC's
# cmake installation.
find_package(gRPC CONFIG REQUIRED)

# After using add_subdirectory, we can now use the grpc targets directly from
# this build.
set(PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(PROTOBUF_PROTOC_EXECUTABLE $<TARGET_FILE:protobuf::protoc>)
set(GRPC_GRPCPP gRPC::grpc++)
set(GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)

set(PROTO_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/proto")
set(PROTO_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${PROTO_OUT_DIR})

file(GLOB PROTO_FILES "${PROTO_SRC_DIR}/*.proto")
foreach(PROTO_FILE ${PROTO_FILES})
  file(RELATIVE_PATH BASE_NAME ${PROTO_SRC_DIR} ${PROTO_FILE})
  string(REGEX REPLACE "[.]proto$" ".pb.cc" PROTO_OUTPUT_SOURCE ${BASE_NAME})
  string(REGEX REPLACE "[.]proto$" ".grpc.pb.cc" GRPC_OUTPUT_SOURCE
                       ${BASE_NAME})
  list(APPEND PROTO_OUTPUT_SOURCES "${PROTO_OUT_DIR}/${PROTO_OUTPUT_SOURCE}")
  list(APPEND PROTO_OUTPUT_SOURCES "${PROTO_OUT_DIR}/${GRPC_OUTPUT_SOURCE}")
endforeach()
add_custom_command(
  OUTPUT ${PROTO_OUTPUT_SOURCES}
  COMMAND
    ${PROTOBUF_PROTOC_EXECUTABLE} ARGS --cpp_out=${PROTO_OUT_DIR}
    --grpc_out=${PROTO_OUT_DIR}
    --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE} -I ${PROTO_SRC_DIR}
    ${PROTO_FILES}
  DEPENDS ${PROTO_FILES}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "some comment")

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

include_directories(src/include)
# Include generated *.pb.h files
include_directories(${PROTO_OUT_DIR})

set(EXTENSION_SOURCES
    src/common/endpoint.cpp
    src/common/hash.cpp
    src/mpp_extension.cpp
    src/mpp_service.cpp
    src/mpp_server.cpp
    src/mpp_client.cpp
    src/execution/mpp_physical_insert.cpp
    src/execution/mpp_physical_update.cpp
    src/execution/mpp_plan_delete.cpp
    src/execution/mpp_plan_insert.cpp
    src/execution/mpp_plan_update.cpp
    src/function/create_distributed_table.cpp
    src/function/master_add_node.cpp
    src/function/mpp_table_scan.cpp
    src/function/prune_shards.cpp
    src/function/remote_query.cpp
    src/function/shuffle_read.cpp
    src/parser/mpp_parse_data.cpp
    src/parser/mpp_parser_extension.cpp
    src/parser/partition_method.cpp
    src/remote_query_result.cpp
    src/shuffle/simple_shuffle_manager.cpp
    src/shuffle/shuffle_manager.cpp
    src/storage/mpp_catalog.cpp
    src/storage/mpp_schema_entry.cpp
    src/storage/mpp_storage_extension.cpp
    src/storage/mpp_system_table.cpp
    src/storage/mpp_table_entry.cpp
    src/storage/mpp_transaction.cpp
    src/storage/mpp_transaction_manager.cpp
    src/storage/mpp_nodes.cpp
    src/storage/mpp_shards.cpp
    src/storage/mpp_tables.cpp
    ${PROTO_OUTPUT_SOURCES})

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} ${GRPC_GRPCPP})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${GRPC_GRPCPP})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
