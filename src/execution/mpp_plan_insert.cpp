#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "common/distributed_table_info.hpp"
#include "common/hash.hpp"
#include "execution/mpp_physical_insert.hpp"
#include "storage/mpp_catalog.hpp"
#include "storage/mpp_table_entry.hpp"
#include "storage/mpp_tables.hpp"
#include "storage/mpp_shards.hpp"

namespace duckdb {
unique_ptr<PhysicalOperator> MppCatalog::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                    unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw NotImplementedException("INSERT with RETURNING is not supported for MPP table!");
	}
	if (op.action_type != OnConflictAction::THROW) {
		throw NotImplementedException("Conflict action %s is not supported for MPP table!", op.action_type);
	}
	auto info = DistributedTableInfo {};
	auto shards = vector<MppShardInfo> {};
	auto shard_locations = vector<Endpoint> {};
	table_manager_->GetDistributedTableInfo(context, op.table.oid, info);
	shard_manager_->GetShards(context, op.table.oid, shards);
	shard_locations.resize(shards.size());
	for (auto &shard : shards) {
		shard_locations[shard.shard_id] = Endpoint::Parse(shard.node);
	}

	auto &partition_column = op.table.GetColumns().GetColumn(info.partition_column);
	auto hash_expression = GetHashPartitionColumnExpression(context, partition_column, info.buckets);
	auto insert = make_uniq<MppPhysicalInsert>(op.types, op.estimated_cardinality, *shuffle_manager_, op.table,
	                                           op.column_index_map, std::move(op.bound_defaults),
	                                           std::move(hash_expression), std::move(shard_locations), local_endpoint_);
	if (plan) {
		insert->children.emplace_back(std::move(plan));
	}
	return insert;
}

} // namespace duckdb
