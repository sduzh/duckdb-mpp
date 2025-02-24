#include "storage/mpp_shards.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "storage/mpp_catalog.hpp"

namespace duckdb {
static const auto kTableName = "shards";
static const auto kTableOIDColumn = ColumnDefinition("table_oid", LogicalType::UBIGINT);
static const auto kShardIdColumn = ColumnDefinition("shard_id", LogicalType::USMALLINT);
static const auto kNodeColumn = ColumnDefinition("node", LogicalType::VARCHAR);

void MppShards::Initialize(ClientContext &context) {
	auto columns = ColumnList {};
	columns.AddColumn(kTableOIDColumn.Copy());
	columns.AddColumn(kShardIdColumn.Copy());
	columns.AddColumn(kNodeColumn.Copy());

	auto constraints = vector<unique_ptr<Constraint>> {};
	constraints.emplace_back(
	    make_uniq<UniqueConstraint>(vector<string> {kTableOIDColumn.GetName(), kShardIdColumn.GetName()}, true));
	constraints.emplace_back(make_uniq<NotNullConstraint>(LogicalIndex(0)));
	constraints.emplace_back(make_uniq<NotNullConstraint>(LogicalIndex(1)));
	constraints.emplace_back(make_uniq<NotNullConstraint>(LogicalIndex(2)));

	for (auto &c : columns.Logical()) {
		column_ids_.emplace_back(c.StorageOid());
	}

	MppSystemTable::Initialize(context, kTableName, std::move(columns), std::move(constraints));
}

MppShards &MppShards::Get(ClientContext &context) {
	return *Catalog::GetCatalog(context, "").Cast<MppCatalog>().GetShardManager();
}

MppShards &MppShards::Get(MppCatalog &mpp_catalog) {
	return *mpp_catalog.GetShardManager();
}

void MppShards::AddShards(ClientContext &context, const vector<MppShardInfo> &shards) {
	auto insert_chunk = DataChunk {};
	insert_chunk.Initialize(context, GetTypes(), shards.size());
	insert_chunk.SetCardinality(shards.size());
	for (size_t i = 0, sz = shards.size(); i < sz; ++i) {
		auto &shard = shards[i];
		insert_chunk.data[0].SetValue(i, Value::UBIGINT(shard.table_oid));
		insert_chunk.data[1].SetValue(i, Value::USMALLINT(shard.shard_id));
		insert_chunk.data[2].SetValue(i, StringVector::AddString(insert_chunk.data[2], shard.node));
	}
	MppSystemTable::Insert(context, insert_chunk);
}

void MppShards::GetShards(ClientContext &context, idx_t table_oid, vector<MppShardInfo> &shards) {
	auto &transaction = DuckTransaction::Get(context, catalog_.GetBase());
	auto state = TableScanState {};
	auto filters = TableFilterSet {};
	filters.PushFilter(ColumnIndex(0),
	                   make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, Value::UBIGINT(table_oid)));
	InitializeScan(transaction, state, {StorageIndex(1), StorageIndex(2)}, nullptr);

	auto chunk = DataChunk {};
	chunk.Initialize(context, GetTypes());
	Scan(transaction, chunk, state);
	if (chunk.size() == 0) {
		return;
	}
	chunk.Flatten();
	shards.resize(chunk.size());
	for (idx_t i = 0; i < chunk.size(); ++i) {
		shards[i].table_oid = table_oid;
	}
	// shard_id
	auto shard_id_data = FlatVector::GetData<uint16_t>(chunk.data[0]);
	for (idx_t i = 0; i < chunk.size(); ++i) {
		shards[i].shard_id = shard_id_data[i];
	}
	// node
	auto node_data = FlatVector::GetData<string_t>(chunk.data[1]);
	for (idx_t i = 0; i < chunk.size(); ++i) {
		shards[i].node = node_data[i].GetString();
	}
}

void MppShards::UpdateShard(duckdb::ClientContext &context, duckdb::idx_t table_oid, idx_t shard_id,
                            const MppShardInfo &shard) {
	throw NotImplementedException("ShardManager::UpdateShards");
}

void MppShards::DeleteShards(ClientContext &context, idx_t table_oid) {
	throw NotImplementedException("ShardManager::DeleteShards");
}

} // namespace duckdb