#pragma once

#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "parser/partition_method.hpp"

namespace duckdb {

struct MppCreateDistributedTableData : public ParserExtensionParseData {
	unique_ptr<CreateTableInfo> base;
	//! The name of the partition column
	string partition_column;
	//! The number of buckets
	uint16_t buckets;

	unique_ptr<ParserExtensionParseData> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb