#include "storage/mpp_transaction.hpp"

#include "storage/mpp_catalog_utils.hpp"
#include "duckdb/main/connection.hpp"
#include "storage/mpp_schema_entry.hpp"
#include "storage/mpp_table_entry.hpp"

namespace duckdb {

MppTransaction::MppTransaction(TransactionManager &manager, ClientContext &context, unique_ptr<Connection> duck_conn)
    : Transaction(manager, context), duck_conn(std::move(duck_conn)) {
}

MppTransaction::~MppTransaction() = default;

MppTransaction &MppTransaction::Get(ClientContext &context, Catalog &catalog) {
	D_ASSERT(catalog.GetCatalogType() == "mpp");
	return Transaction::Get(context, catalog).Cast<MppTransaction>();
}

ClientContext &MppTransaction::GetDuckContext() {
	return *duck_conn->context;
}

MppTableEntry &MppTransaction::WrapDuckTable(MppCatalog &catalog, MppSchemaEntry &schema, TableCatalogEntry &table) {
	D_ASSERT(table.IsDuckTable());
	D_ASSERT(IsMppCatalogEntry(table));
	auto guard = lock_guard(lock_);
	if (auto iter = entry_map_.find(table); iter != entry_map_.end()) {
		return iter->second->Cast<MppTableEntry>();
	} else {
		auto result = entry_map_.emplace(table, MppTableEntry::WrapDuckTable(catalog, schema, table));
		return result.first->second->Cast<MppTableEntry>();
	}
}

MppSchemaEntry &MppTransaction::WrapDuckSchema(MppCatalog &catalog, SchemaCatalogEntry &schema) {
	D_ASSERT(IsMppCatalogEntry(schema));
	auto guard = lock_guard(lock_);
	if (auto iter = entry_map_.find(schema); iter != entry_map_.end()) {
		return iter->second->Cast<MppSchemaEntry>();
	} else {
		auto result = entry_map_.emplace(schema, MppSchemaEntry::WrapDuckSchema(catalog, schema));
		return result.first->second->Cast<MppSchemaEntry>();
	}
}

void MppTransaction::DropTable(idx_t oid) {
	auto guard = lock_guard(lock_);
	dropped_tables_.push_back(oid);
}

} // namespace duckdb