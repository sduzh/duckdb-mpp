#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

class CatalogEntry;
class Connection;
class MppCatalog;
class MppSchemaEntry;
class MppTableEntry;
class SchemaCatalogEntry;
class TableCatalogEntry;

class MppTransaction final : public Transaction {
public:
	MppTransaction(TransactionManager &manager, ClientContext &context, unique_ptr<Connection> duck_conn);

	~MppTransaction() override;

	static MppTransaction &Get(ClientContext &context, Catalog &catalog);

	ClientContext &GetDuckContext();

	MppSchemaEntry &WrapDuckSchema(MppCatalog &catalog, SchemaCatalogEntry &schema);

	MppTableEntry &WrapDuckTable(MppCatalog &catalog, MppSchemaEntry &schema, TableCatalogEntry &table);

	void DropTable(idx_t oid);

	unique_ptr<Connection> duck_conn;
	transaction_t transaction_id;

	vector<idx_t> GetDroppedTables() const {
		return dropped_tables_;
	}

private:
	mutex lock_;
	reference_map_t<CatalogEntry, unique_ptr<CatalogEntry>> entry_map_;
	vector<idx_t> dropped_tables_;
};
} // namespace duckdb