#pragma once

#include "common/constants.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/catalog/catalog_entry.hpp"

namespace duckdb {
class ColumnDefinition;
class BoundConstantExpression;

inline void AddMppTag(optional_ptr<CreateInfo> info) {
	info->tags.emplace(MPP_TAG, MPP_VERSION);
}

inline bool IsMppCatalogEntry(optional_ptr<CatalogEntry> entry) {
	return entry && entry->tags.contains(MPP_TAG);
}

template <bool WITH_SCHEMA>
inline string ShardTableName(idx_t table_oid, uint16_t shard_id) {
	if constexpr (WITH_SCHEMA) {
		return StringUtil::Format("%s.shard_%d_%d", MPP_SHARDS_SCHEMA, table_oid, shard_id);
	} else {
		return StringUtil::Format("shard_%d_%d", table_oid, shard_id);
	}
}

} // namespace duckdb