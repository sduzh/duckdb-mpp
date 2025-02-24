#include "parser/partition_method.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {
const StringUtil::EnumStringLiteral *GetPartitionMethodValues() {
	static constexpr StringUtil::EnumStringLiteral values[] {
	    {static_cast<uint32_t>(PartitionMethod::HASH), "HASH"},
	};
	return values;
}

template <>
const char *EnumUtil::ToChars<PartitionMethod>(PartitionMethod value) {
	return StringUtil::EnumToString(GetPartitionMethodValues(), 1, "PartitionMethod", static_cast<uint32_t>(value));
}

template <>
PartitionMethod EnumUtil::FromString<PartitionMethod>(const char *value) {
	return static_cast<PartitionMethod>(
	    StringUtil::StringToEnum(GetPartitionMethodValues(), 1, "PartitionMethod", value));
}

} // namespace duckdb