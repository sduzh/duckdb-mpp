#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/typedefs.hpp"
#include "parser/partition_method.hpp"

namespace duckdb {

struct DistributedTableInfo {
	DistributedTableInfo()
	    : table_oid(DConstants::INVALID_INDEX), partition_column(DConstants::INVALID_INDEX), buckets(0),
	      deleted(false) {
	}

	idx_t table_oid;
	PhysicalIndex partition_column;
	uint16_t buckets;
	bool deleted;
};

} // namespace duckdb