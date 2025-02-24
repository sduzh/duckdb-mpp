#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

struct MppShardInfo {
	idx_t table_oid;
	uint16_t shard_id;
	string node;

	bool operator==(const MppShardInfo &rhs) const noexcept {
		return table_oid == rhs.table_oid && shard_id == rhs.shard_id && node == rhs.node;
	}

	bool IsValid() const {
		return !node.empty();
	}
};

} // namespace duckdb