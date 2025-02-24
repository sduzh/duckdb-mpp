#include "storage/mpp_schema_entry.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "common/distributed_table_info.hpp"
#include "storage/mpp_catalog.hpp"
#include "storage/mpp_catalog_utils.hpp"
#include "storage/mpp_table_entry.hpp"
#include "storage/mpp_tables.hpp"
#include "storage/mpp_transaction.hpp"

namespace duckdb {
bool MppSchemaEntry::IsInternalName(const string &name) {
	return name.starts_with("__mpp_");
}

string MppSchemaEntry::GetInternalName(const string &name) {
	return IsInternalName(name) ? name : "__mpp_" + name;
}

string MppSchemaEntry::GetUserName(const string &name) {
	return IsInternalName(name) ? name.substr(strlen("__mpp_")) : name;
}

unique_ptr<MppSchemaEntry> MppSchemaEntry::WrapDuckSchema(MppCatalog &catalog, SchemaCatalogEntry &base) {
	D_ASSERT(MppCatalog::IsInternalSchema(base.name));
	auto info = unique_ptr_cast<CreateInfo, CreateSchemaInfo>(base.GetInfo());
	info->catalog = catalog.GetName();
	info->schema = GetUserName(base.name);
	auto ret = make_uniq<MppSchemaEntry>(catalog, *info, base);
	ret->oid = base.oid;
	return ret;
}

MppSchemaEntry::MppSchemaEntry(MppCatalog &catalog, CreateSchemaInfo &info, SchemaCatalogEntry &schema)
    : SchemaCatalogEntry(catalog, info), schema_(schema) {
	D_ASSERT(MppCatalog::IsInternalSchema(schema_.name));
}

MppSchemaEntry::~MppSchemaEntry() = default;

void MppSchemaEntry::Scan(ClientContext &context, CatalogType type,
                          const std::function<void(CatalogEntry &)> &callback) {
	auto &mpp_transaction = MppTransaction::Get(context, catalog);
	auto &duck_context = mpp_transaction.GetDuckContext();
	schema_.Scan(duck_context, type, [&](CatalogEntry &e) {
		if (!IsMppCatalogEntry(e)) {
			D_ASSERT(!IsInternalName(e.name));
			return;
		} else if (type == CatalogType::TABLE_ENTRY) {
			auto &duck_table = e.Cast<TableCatalogEntry>();
			auto &wrapped = mpp_transaction.WrapDuckTable(GetMppCatalog(), *this, duck_table);
			callback(wrapped);
		} else {
			callback(e);
		}
	});
}

void MppSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	D_ASSERT(type != CatalogType::TABLE_ENTRY);
	schema_.Scan(type, [&](CatalogEntry &e) {
		if (IsMppCatalogEntry(e)) {
			callback(e);
		}
	});
}

optional_ptr<CatalogEntry> MppSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                       TableCatalogEntry &table) {
	throw NotImplementedException("MppSchemaEntry::CreateIndex");
}

optional_ptr<CatalogEntry> MppSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw NotImplementedException("MppSchemaEntry::CreateFunction");
}

optional_ptr<CatalogEntry> MppSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto new_bound_info = BoundCreateTableInfo(schema_, info.base->Copy());
	new_bound_info.base->catalog = schema_.catalog.GetName();
	new_bound_info.base->schema = schema_.name;
	AddMppTag(new_bound_info.base);

	auto &mpp_transaction = transaction.transaction->Cast<MppTransaction>();
	auto &duck_context = mpp_transaction.GetDuckContext();
	auto duck_transaction = schema_.catalog.GetCatalogTransaction(duck_context);
	MetaTransaction::Get(duck_context).ModifyDatabase(schema_.catalog.GetAttached());
	auto e = schema_.CreateTable(duck_transaction, new_bound_info);
	if (!e) {
		return {};
	}
	D_ASSERT(IsMppCatalogEntry(e));
	return mpp_transaction.WrapDuckTable(GetMppCatalog(), *this, e->Cast<TableCatalogEntry>());
}

optional_ptr<CatalogEntry> MppSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("MppSchemaEntry::CreateView");
}

optional_ptr<CatalogEntry> MppSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw NotImplementedException("MppSchemaEntry::CreateSequence");
}

optional_ptr<CatalogEntry> MppSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                               CreateTableFunctionInfo &info) {
	throw NotImplementedException("MppSchemaEntry::CreateTableFunction");
}

optional_ptr<CatalogEntry> MppSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                              CreateCopyFunctionInfo &info) {
	throw NotImplementedException("MppSchemaEntry::CreateCopyFunction");
}

optional_ptr<CatalogEntry> MppSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("MppSchemaEntry::CreatePragmaFunction");
}

optional_ptr<CatalogEntry> MppSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw NotImplementedException("MppSchemaEntry::CreateCollation");
}

optional_ptr<CatalogEntry> MppSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("MppSchemaEntry::CreateType");
}

optional_ptr<CatalogEntry> MppSchemaEntry::GetEntry(CatalogTransaction transaction, CatalogType type,
                                                    const string &name) {
	auto &mpp_transaction = transaction.transaction->Cast<MppTransaction>();
	auto duck_transaction = schema_.catalog.GetCatalogTransaction(mpp_transaction.GetDuckContext());
	auto e = schema_.GetEntry(duck_transaction, type, name);
	if (!IsMppCatalogEntry(e)) {
		return {};
	}
	return mpp_transaction.WrapDuckTable(GetMppCatalog(), *this, e->Cast<TableCatalogEntry>());
}

void MppSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	auto &transaction = MppTransaction::Get(context, GetMppCatalog());
	auto &duck_context = transaction.GetDuckContext();
	auto &duck_catalog = schema_.catalog;
	auto duck_catalog_transaction = duck_catalog.GetCatalogTransaction(duck_context);
	auto new_info = info.Copy();
	new_info->catalog = duck_catalog.GetName();
	new_info->schema = schema_.name;
	auto dropped_table = schema_.GetEntry(duck_catalog_transaction, CatalogType::TABLE_ENTRY, info.name);
	MetaTransaction::Get(duck_context).ModifyDatabase(schema_.catalog.GetAttached());
	schema_.DropEntry(transaction.GetDuckContext(), *new_info);
	if (dropped_table) {
		MppTables::Get(GetMppCatalog()).MarkDeleteTable(duck_context, dropped_table->oid);
		transaction.DropTable(dropped_table->oid);
	}
}

void MppSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("MppSchemaEntry::Alter");
}

MppCatalog &MppSchemaEntry::GetMppCatalog() {
	return catalog.Cast<MppCatalog>();
}

} // namespace duckdb