#pragma once

#include "duckdb/function/table_function.hpp"
#include "storage/mpp_shard_info.hpp"

namespace duckdb {

struct MppTableScanBindData : public TableFunctionData {
	explicit MppTableScanBindData(ClientContext &contest, TableCatalogEntry &table);

	explicit MppTableScanBindData(TableCatalogEntry &table);

	~MppTableScanBindData() override;

	//! The table to scan.
	TableCatalogEntry &table;
	optional_ptr<const ColumnDefinition> partition_column;
	int32_t buckets;
	vector<MppShardInfo> shards;
	vector<unique_ptr<Expression>> filters;
	string where_clause;

public:
	bool Equals(const FunctionData &other_p) const override;

	unique_ptr<FunctionData> Copy() const override;
};

struct MppTableScanFunction {
	static TableFunction GetFunction();
};
} // namespace duckdb