#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

class MppCatalog;
class MppTableEntry;

class MppSchemaEntry final : public SchemaCatalogEntry {
public:
	MppSchemaEntry(MppCatalog &catalog, CreateSchemaInfo &info, SchemaCatalogEntry &schema);

	~MppSchemaEntry() override;

	static unique_ptr<MppSchemaEntry> WrapDuckSchema(MppCatalog &catalog, SchemaCatalogEntry &base);

	static string GetInternalName(const string &name);

	static string GetUserName(const string &name);

	static bool IsInternalName(const string &name);

	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;

	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;

	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;

	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;

	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;

	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;

	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;

	optional_ptr<CatalogEntry> GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) override;

	void DropEntry(ClientContext &context, DropInfo &info) override;

	void Alter(CatalogTransaction transaction, AlterInfo &info) override;

	MppCatalog &GetMppCatalog();

private:
	SchemaCatalogEntry &schema_;
};

} // namespace duckdb