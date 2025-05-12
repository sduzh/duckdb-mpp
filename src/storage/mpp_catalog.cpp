#include "storage/mpp_catalog.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/storage/database_size.hpp"
#include "mpp_server.hpp"
#include "execution/mpp_physical_insert.hpp"
#include "shuffle/shuffle_manager.hpp"
#include "storage/mpp_catalog_utils.hpp"
#include "common/distributed_table_info.hpp"
#include "storage/mpp_nodes.hpp"
#include "storage/mpp_schema_entry.hpp"
#include "storage/mpp_shards.hpp"
#include "storage/mpp_tables.hpp"
#include "storage/mpp_transaction.hpp"

namespace duckdb {

MppCatalog::MppCatalog(AttachedDatabase &database, Catalog &base, Endpoint local_endpoint)
    : Catalog(database), base_(base), local_endpoint_(std::move(local_endpoint)),
      node_manager_(make_uniq<MppNodes>(*this)), shard_manager_(make_uniq<MppShards>(*this)),
      table_manager_(make_uniq<MppTables>(*this)), mpp_server_(make_uniq<MppServer>(*this)),
      shuffle_manager_(ShuffleManager::New(ShuffleManagerType::SIMPLE)) {
}

MppCatalog::~MppCatalog() {
	if (mpp_server_) {
		mpp_server_->StopServer();
	}
}

void MppCatalog::Initialize(bool load_builtin) {
	mpp_server_->StartServer(local_endpoint_);

	auto conn = Connection(base_.GetDatabase());
	auto &context = *conn.context;
	context.transaction.BeginTransaction();

	MetaTransaction::Get(context).ModifyDatabase(base_.GetAttached());

	InitDefaultSchema(context);

	node_manager_->Initialize(context);
	shard_manager_->Initialize(context);
	table_manager_->Initialize(context);

	context.transaction.Commit();
}

void MppCatalog::InitDefaultSchema(ClientContext &context) {
	auto info = CreateSchemaInfo {};
	info.catalog = base_.GetName();
	info.schema = MppSchemaEntry::GetInternalName(DEFAULT_SCHEMA);
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	AddMppTag(info);
	base_.CreateSchema(context, info);
}

optional_ptr<CatalogEntry> MppCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	if (info.temporary) {
		throw InvalidInputException("MPP does not support temporary schema");
	}
	if (info.internal) {
		throw InvalidInputException("MPP does not support internal schema");
	}
	auto &mpp_transaction = transaction.transaction->Cast<MppTransaction>();

	auto new_info = info.Copy();
	new_info->catalog = base_.GetName();
	new_info->schema = InternalSchemaName(info.schema);
	AddMppTag(new_info);

	MetaTransaction::Get(mpp_transaction.GetDuckContext()).ModifyDatabase(base_.GetAttached());
	auto e = base_.CreateSchema(mpp_transaction.GetDuckContext(), new_info->Cast<CreateSchemaInfo>());
	if (!e) {
		return e;
	}
	D_ASSERT(IsMppCatalogEntry(e));
	return mpp_transaction.WrapDuckSchema(*this, e->Cast<SchemaCatalogEntry>());
}

optional_ptr<SchemaCatalogEntry> MppCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name,
                                                       OnEntryNotFound if_not_found, QueryErrorContext error_context) {
	auto &mpp_transaction = transaction.transaction->Cast<MppTransaction>();
	auto real_schema_name = InternalSchemaName(schema_name);
	auto e = base_.GetSchema(mpp_transaction.GetDuckContext(), real_schema_name, if_not_found, error_context);
	if (!IsMppCatalogEntry(e.get())) {
		return {};
	}
	return mpp_transaction.WrapDuckSchema(*this, e->Cast<SchemaCatalogEntry>());
}

void MppCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	auto &transaction = MppTransaction::Get(context, *this);
	base_.ScanSchemas(transaction.GetDuckContext(), [&](SchemaCatalogEntry &e) {
		if (IsMppCatalogEntry(e)) {
			callback(transaction.WrapDuckSchema(*this, e));
		}
	});
}

unique_ptr<PhysicalOperator> MppCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
                                                           unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("MppCatalog:PlanCreateTableAs");
}

DatabaseSize MppCatalog::GetDatabaseSize(ClientContext &context) {
	return {};
}

vector<MetadataBlockInfo> MppCatalog::GetMetadataInfo(ClientContext &context) {
	return {};
}

bool MppCatalog::InMemory() {
	return false;
}

string MppCatalog::GetDBPath() {
	return "mpp:" + base_.GetName();
}

void MppCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("MppCatalog::DropSchema");
}

bool MppCatalog::IsInternalSchema(const string &name) {
	return MppSchemaEntry::IsInternalName(name);
}

string MppCatalog::InternalSchemaName(const string &schema) {
	return MppSchemaEntry::GetInternalName(schema);
}

} // namespace duckdb