#include "storage/mpp_system_table.hpp"

#include "common/constants.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/update_state.hpp"
#include "storage/mpp_catalog.hpp"
#include "storage/mpp_schema_entry.hpp"

namespace duckdb {

MppSystemTable::~MppSystemTable() = default;

void MppSystemTable::Initialize(ClientContext &context, const string &table_name, ColumnList columns,
                                vector<unique_ptr<Constraint>> constraints) {
	D_ASSERT(!table_);

	columns.Finalize();

	MetaTransaction::Get(context).ModifyDatabase(catalog_.GetBase().GetAttached());

	auto create_schema = CreateSchemaInfo {};
	create_schema.schema = MppSchemaEntry::GetInternalName(MPP_SYSTEM_SCHEMA);
	create_schema.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog_.GetBase().CreateSchema(context, create_schema);
	auto &schema = catalog_.GetBase().GetSchema(context, create_schema.schema);

	auto create_table = make_uniq<CreateTableInfo>(schema, table_name);
	create_table->columns = std::move(columns);
	create_table->constraints = std::move(constraints);
	create_table->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	auto bound_create = BoundCreateTableInfo(schema, std::move(create_table));
	schema.CreateTable(catalog_.GetBase().GetCatalogTransaction(context), bound_create);

	table_ = catalog_.GetBase().GetEntry<TableCatalogEntry>(context, schema.name, table_name);
	D_ASSERT(table_);
}

const ColumnDefinition &MppSystemTable::GetColumn(const string &name) {
	return table_->GetColumn(name);
}

void MppSystemTable::Insert(ClientContext &context, DataChunk &data) {
	D_ASSERT(data.GetTypes() == table_->GetTypes());
	auto &data_table = table_->GetStorage();
	auto append_state = LocalAppendState {};
	auto binder = Binder::CreateBinder(context);
	auto bound_constraints = binder->BindConstraints(*table_);

	ModifyAttachedDatabase(context);

	data_table.InitializeLocalAppend(append_state, *table_, context, bound_constraints);
	data_table.LocalAppend(append_state, context, data, false);
	data_table.FinalizeLocalAppend(append_state);
}

vector<LogicalType> MppSystemTable::GetTypes() const {
	D_ASSERT(table_);
	return table_->GetTypes();
}

void MppSystemTable::InitializeScan(DuckTransaction &transaction, TableScanState &state,
                                    const vector<StorageIndex> &column_ids, TableFilterSet *table_filters) {
	D_ASSERT(table_);
	table_->GetStorage().InitializeScan(transaction, state, column_ids, table_filters);
}

unique_ptr<TableUpdateState>
MppSystemTable::InitializeUpdate(ClientContext &context, const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
	D_ASSERT(table_);
	return table_->GetStorage().InitializeUpdate(*table_, context, bound_constraints);
}

void MppSystemTable::Scan(DuckTransaction &transaction, DataChunk &result, TableScanState &state) {
	D_ASSERT(table_);
	table_->GetStorage().Scan(transaction, result, state);
}

void MppSystemTable::Update(TableUpdateState &state, ClientContext &context, Vector &row_ids,
                            const vector<PhysicalIndex> &column_ids, DataChunk &data) {
	D_ASSERT(table_);
	ModifyAttachedDatabase(context);
	table_->GetStorage().Update(state, context, row_ids, column_ids, data);
}

void MppSystemTable::ModifyAttachedDatabase(ClientContext &context) {
	D_ASSERT(table_);
	MetaTransaction::Get(context).ModifyDatabase(table_->ParentCatalog().GetAttached());
}
} // namespace duckdb