#include "storage/mpp_tables.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "common/distributed_table_info.hpp"
#include "storage/mpp_catalog.hpp"

namespace duckdb {

static const auto kTableName = "tables";
static const auto kTableOIDColumn = ColumnDefinition("table_oid", LogicalType::UBIGINT);
static const auto kPartitionColumnIndexColumn = ColumnDefinition("partition_column_index", LogicalType::UBIGINT);
static const auto kBucketsColumn = ColumnDefinition("buckets", LogicalType::USMALLINT);
static const auto kDeletedColumn = ColumnDefinition("deleted", LogicalType::BOOLEAN);

void MppTables::Initialize(ClientContext &context) {
	auto columns = ColumnList {};
	columns.AddColumn(kTableOIDColumn.Copy());
	columns.AddColumn(kPartitionColumnIndexColumn.Copy());
	columns.AddColumn(kBucketsColumn.Copy());
	columns.AddColumn(kDeletedColumn.Copy());

	auto constraints = vector<unique_ptr<Constraint>> {};
	constraints.emplace_back(make_uniq<UniqueConstraint>(vector<string> {kTableOIDColumn.GetName()}, true));
	constraints.emplace_back(make_uniq<NotNullConstraint>(LogicalIndex(0)));

	for (auto &c : columns.Logical()) {
		column_ids_.emplace_back(c.StorageOid());
	}

	MppSystemTable::Initialize(context, kTableName, std::move(columns), std::move(constraints));
}

MppTables &MppTables::Get(ClientContext &context) {
	return Get(Catalog::GetCatalog(context, "").Cast<MppCatalog>());
}

MppTables &MppTables::Get(MppCatalog &mpp_catalog) {
	return *mpp_catalog.GetTableManager();
}

void MppTables::AddDistributedTable(ClientContext &context, const DistributedTableInfo &info) {
	D_ASSERT(info.partition_column.index != DConstants::INVALID_INDEX);

	auto insert_chunk = DataChunk {};
	insert_chunk.Initialize(context, GetTypes());
	insert_chunk.SetCardinality(1);
	insert_chunk.data[0].SetValue(0, Value::UBIGINT(info.table_oid));
	insert_chunk.data[1].SetValue(0, Value::UBIGINT(info.partition_column.index));
	insert_chunk.data[2].SetValue(0, Value::USMALLINT(info.buckets));
	insert_chunk.data[3].SetValue(0, Value::BOOLEAN(info.deleted));

	Insert(context, insert_chunk);
}

void MppTables::GetDistributedTableInfo(ClientContext &context, idx_t table_oid, DistributedTableInfo &info) {
	if (!TryGetDistributedTableInfo(context, table_oid, info)) {
		throw InternalException("No distributed table with the provided oid: %d", table_oid);
	}
}

bool MppTables::TryGetDistributedTableInfo(ClientContext &context, idx_t table_oid, DistributedTableInfo &info) {
	auto &transaction = DuckTransaction::Get(context, catalog_.GetBase());
	auto state = TableScanState {};
	auto filters = TableFilterSet {};
	filters.PushFilter(ColumnIndex(0),
	                   make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, Value::UBIGINT(table_oid)));
	InitializeScan(transaction, state, column_ids_, nullptr);

	auto chunk = DataChunk {};
	chunk.Initialize(context, GetTypes());
	Scan(transaction, chunk, state);
	if (chunk.size() == 0) {
		return false;
	}
	D_ASSERT(chunk.size() == 1);
	info.table_oid = UBigIntValue::Get(chunk.GetValue(0, 0));
	info.partition_column.index = UBigIntValue::Get(chunk.GetValue(1, 0));
	info.buckets = USmallIntValue::Get(chunk.GetValue(2, 0));
	info.deleted = BooleanValue::Get(chunk.GetValue(3, 0));
	return true;
}

void MppTables::MarkDeleteTable(ClientContext &context, idx_t table_oid) {
	auto &transaction = DuckTransaction::Get(context, catalog_.GetBase());
	auto scan_state = TableScanState {};
	auto filters = TableFilterSet {};
	filters.PushFilter(ColumnIndex(0),
	                   make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, Value::UBIGINT(table_oid)));
	InitializeScan(transaction, scan_state, {/*row id*/ StorageIndex()}, nullptr);

	auto chunk = DataChunk {};
	chunk.Initialize(context, {LogicalType::ROW_TYPE});
	Scan(transaction, chunk, scan_state);
	if (chunk.size() == 0) {
		return;
	}
	D_ASSERT(chunk.size() == 1);
	auto &row_ids = chunk.data[0];

	auto binder = Binder::CreateBinder(context);
	auto bound_constraints = binder->BindConstraints(*table_);
	auto update_state = InitializeUpdate(context, bound_constraints);

	auto &del_column = GetColumn(kDeletedColumn.GetName());
	auto update_column_ids = vector<PhysicalIndex> {del_column.Physical()};
	auto update_chunk = DataChunk {};
	update_chunk.Initialize(context, {LogicalType::BOOLEAN});
	update_chunk.SetCardinality(1);
	update_chunk.SetValue(0, 0, Value::BOOLEAN(true));

	Update(*update_state, context, row_ids, update_column_ids, update_chunk);
}
} // namespace duckdb