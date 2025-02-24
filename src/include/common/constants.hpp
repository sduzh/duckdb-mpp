#pragma once

#include <cstdint>

namespace duckdb {
constexpr const auto MPP_TAG = "__mpp";
constexpr const auto MPP_VERSION = "0.1";
constexpr const auto MPP_SYSTEM_SCHEMA = "system";
constexpr const auto MPP_SHARDS_SCHEMA = "__mpp_shards";
constexpr const auto MPP_SERVER_DEFAULT_PORT = (uint16_t)54321;
} // namespace duckdb