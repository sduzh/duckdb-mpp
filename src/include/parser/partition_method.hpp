#pragma once

#include "duckdb/common/enum_util.hpp"

namespace duckdb {

enum class PartitionMethod : int { HASH };

template <>
const char *EnumUtil::ToChars<PartitionMethod>(PartitionMethod value);

template <>
PartitionMethod EnumUtil::FromString(const char *value);

} // namespace duckdb