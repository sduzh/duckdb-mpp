#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

class MppCatalog;
class MppSchemaEntry;

class MppTableEntry : public TableCatalogEntry {
public:
	MppTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, TableCatalogEntry &base)
	    : TableCatalogEntry(catalog, schema, info), base_(base) {
	}

	static unique_ptr<MppTableEntry> WrapDuckTable(MppCatalog &catalog, MppSchemaEntry &schema,
	                                               TableCatalogEntry &base);

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

private:
	TableCatalogEntry &base_;
};

} // namespace duckdb