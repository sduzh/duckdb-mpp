#pragma once

#include "storage/mpp_system_table.hpp"

#include "duckdb/storage/storage_index.hpp"

namespace duckdb {

class DistributedTableInfo;

class MppTables : public MppSystemTable {
public:
	explicit MppTables(MppCatalog &catalog) : MppSystemTable(catalog) {
	}

	static MppTables &Get(ClientContext &mpp_catalog);

	static MppTables &Get(MppCatalog &mpp_catalog);

	void Initialize(ClientContext &context);

	void AddDistributedTable(ClientContext &context, const DistributedTableInfo &info);

	void GetDistributedTableInfo(ClientContext &context, idx_t table_oid, DistributedTableInfo &info);

	bool TryGetDistributedTableInfo(ClientContext &context, idx_t table_oid, DistributedTableInfo &info);

	void MarkDeleteTable(ClientContext &context, idx_t table_oid);

	void RemoveTable(ClientContext &context, idx_t table_oid);

private:
	vector<StorageIndex> column_ids_;
};

} // namespace duckdb