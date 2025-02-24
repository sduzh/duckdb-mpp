#include "execution/physical_shuffle_write.hpp"
#include "common/endpoint.hpp"
#include "common/serializer.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "mpp_client.hpp"
#include "remote_query_result.hpp"
#include "shuffle/shuffle_manager.hpp"
#include "storage/mpp_catalog_utils.hpp"

namespace duckdb {
struct GlobalShuffleWriteState final : public GlobalSinkState {
	explicit GlobalShuffleWriteState(ShuffleManager &shuffle_manager, const TableCatalogEntry &table,
	                                 const vector<Endpoint> &shard_locations, Endpoint local_endpoint,
	                                 partition_id_t min_partition_id)
	    : shuffle_manager(shuffle_manager), table(table), shard_locations(shard_locations),
	      local_endpoint(std::move(local_endpoint)), min_partition_id(min_partition_id), insert_count(0) {
	}

	void OpenPartition(partition_id_t partition_id) {
		auto l = lock_guard(lock);
		if (remote_query_results.contains(partition_id)) {
			return;
		}
		shuffle_manager.OpenPartition(partition_id);
		D_ASSERT(partition_id >= min_partition_id);
		auto shard_index = partition_id - min_partition_id;
		D_ASSERT(shard_index < shard_locations.size());
		auto &location = shard_locations[shard_index];
		auto query = StringUtil::Format("INSERT INTO %s SELECT * FROM shuffle_read('%s', %d, '%s');",
		                                ShardTableName<true>(table.oid, shard_index), local_endpoint.ToString(),
		                                partition_id, ShardTableName<false>(table.oid, shard_index));
		auto client = MppClient::NewClient(location);
		auto result = client->Query(query, false);
		remote_query_results.emplace(partition_id, std::move(result));
	}

	mutex lock;
	ShuffleManager &shuffle_manager;
	const TableCatalogEntry &table;
	const vector<Endpoint> shard_locations;
	Endpoint local_endpoint;
	partition_id_t min_partition_id;
	idx_t insert_count;
	unordered_map<partition_id_t, std::unique_ptr<RemoteQueryResult>> remote_query_results;
};

struct LocalShuffleWriteState final : public LocalSinkState {
	explicit LocalShuffleWriteState(ClientContext &context, const vector<LogicalType> &types,
	                                const vector<unique_ptr<Expression>> &bound_defaults,
	                                const Expression &hash_expression)
	    : default_executor(context, bound_defaults), hash_executor(context, hash_expression),
	      hash_vector(hash_expression.return_type, true, false) {
		insert_chunk.Initialize(context, types);
	}

	void ResetPartitionSelections(idx_t num_partitions) {
		partition_selections.resize(num_partitions);
		for (auto &v : partition_selections) {
			v.Initialize();
		}
		selection_sizes.clear();
		selection_sizes.resize(num_partitions);
	}

	ExpressionExecutor default_executor;
	ExpressionExecutor hash_executor;
	//! The vector that stored hash values of partition column
	Vector hash_vector;
	//! The chunk that ends up getting inserted
	DataChunk insert_chunk;
	vector<SelectionVector> partition_selections;
	vector<idx_t> selection_sizes;
	unordered_set<partition_id_t> opened_partitions;
};

PhysicalShuffleWrite::PhysicalShuffleWrite(vector<LogicalType> types, idx_t estimated_cardinality,
                                           ShuffleManager &shuffle_manager, TableCatalogEntry &table,
                                           physical_index_vector_t<idx_t> column_index_map,
                                           vector<unique_ptr<Expression>> bound_defaults,
                                           unique_ptr<Expression> hash_expression, vector<Endpoint> shard_locations,
                                           Endpoint local_endpoint)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      shuffle_manager(shuffle_manager), column_index_map(std::move(column_index_map)), insert_table(&table),
      insert_types(table.GetTypes()), bound_defaults(std::move(bound_defaults)),
      hash_expression(std::move(hash_expression)), shard_locations(std::move(shard_locations)),
      local_endpoint(std::move(local_endpoint)) {
	first_partition_id = shuffle_manager.GetPartitionId(NumericCast<int32_t>(this->shard_locations.size()));
}

PhysicalShuffleWrite::~PhysicalShuffleWrite() = default;

unique_ptr<GlobalSinkState> PhysicalShuffleWrite::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<GlobalShuffleWriteState>(shuffle_manager, *insert_table, shard_locations, local_endpoint,
	                                          first_partition_id);
}

unique_ptr<LocalSinkState> PhysicalShuffleWrite::GetLocalSinkState(ExecutionContext &context) const {
	auto result = make_uniq<LocalShuffleWriteState>(context.client, insert_types, bound_defaults, *hash_expression);
	return result;
}

SinkResultType PhysicalShuffleWrite::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<GlobalShuffleWriteState>();
	auto &lstate = input.local_state.Cast<LocalShuffleWriteState>();

	ResolveDefaults(*insert_table, chunk, column_index_map, lstate.default_executor, lstate.insert_chunk);

	gstate.insert_count += lstate.insert_chunk.size();

	// Calculate hash values
	lstate.hash_vector.Initialize(false, lstate.insert_chunk.size());
	lstate.hash_executor.ExecuteExpression(lstate.insert_chunk, lstate.hash_vector);
	lstate.hash_vector.Flatten(lstate.insert_chunk.size());
	D_ASSERT(lstate.hash_vector.GetType() == LogicalType::USMALLINT);

	lstate.ResetPartitionSelections(shard_locations.size());
	auto hash_data = FlatVector::GetData<uint16_t>(lstate.hash_vector);
	for (idx_t i = 0, sz = lstate.insert_chunk.size(); i < sz; ++i) {
		auto h = hash_data[i];
		auto &size = lstate.selection_sizes[h];
		lstate.partition_selections[h].set_index(size++, i);
	}

	for (idx_t i = 0, sz = lstate.selection_sizes.size(); i < sz; ++i) {
		auto sel_size = lstate.selection_sizes[i];
		if (sel_size == 0) {
			continue;
		}
		auto partition_id = first_partition_id + i;
		if (!lstate.opened_partitions.contains(partition_id)) {
			gstate.OpenPartition(partition_id);
			lstate.opened_partitions.insert(partition_id);
		}
		auto &sel = lstate.partition_selections[i];
		auto partition_chunk = DataChunk();
		partition_chunk.Initialize(context.client, insert_types, sel_size);
		lstate.insert_chunk.Copy(partition_chunk, sel, sel_size);
		D_ASSERT(partition_chunk.size() == sel_size);

		auto data = SerializeToString(partition_chunk);
		shuffle_manager.BlockWrite(partition_id, std::move(data));
	}

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalShuffleWrite::ResolveDefaults(const TableCatalogEntry &table, DataChunk &chunk,
                                           const physical_index_vector_t<idx_t> &column_index_map,
                                           ExpressionExecutor &default_executor, DataChunk &result) {
	chunk.Flatten();
	default_executor.SetChunk(chunk);

	result.Reset();
	result.SetCardinality(chunk);

	if (!column_index_map.empty()) {
		// columns specified by the user, use column_index_map
		for (auto &col : table.GetColumns().Physical()) {
			auto storage_idx = col.StorageOid();
			auto mapped_index = column_index_map[col.Physical()];
			if (mapped_index == DConstants::INVALID_INDEX) {
				// insert default value
				default_executor.ExecuteExpression(storage_idx, result.data[storage_idx]);
			} else {
				// get value from child chunk
				D_ASSERT((idx_t)mapped_index < chunk.ColumnCount());
				D_ASSERT(result.data[storage_idx].GetType() == chunk.data[mapped_index].GetType());
				result.data[storage_idx].Reference(chunk.data[mapped_index]);
			}
		}
	} else {
		// no columns specified, just append directly
		for (idx_t i = 0; i < result.ColumnCount(); i++) {
			D_ASSERT(result.data[i].GetType() == chunk.data[i].GetType());
			result.data[i].Reference(chunk.data[i]);
		}
	}
}

SinkCombineResultType PhysicalShuffleWrite::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalShuffleWrite::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<GlobalShuffleWriteState>();
	auto error = string {};
	for (auto &[id, _] : gstate.remote_query_results) {
		shuffle_manager.ClosePartition(id);
	}
	for (auto &[id, result] : gstate.remote_query_results) {
		result->Fetch();
		if (result->HasError() && error.empty()) {
			error = result->GetError();
		}
	}
	for (auto &[id, _] : gstate.remote_query_results) {
		shuffle_manager.RemotePartition(id);
	}
	if (!error.empty()) {
		throw ExecutorException(error);
	}
	return SinkFinalizeType::READY;
}

struct GlobalShuffleWriteSourceState : public GlobalSourceState {};

unique_ptr<GlobalSourceState> PhysicalShuffleWrite::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<GlobalShuffleWriteSourceState>();
}

SourceResultType PhysicalShuffleWrite::GetData(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<GlobalShuffleWriteState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(insert_gstate.insert_count)));
	return SourceResultType::FINISHED;
}

} // namespace duckdb