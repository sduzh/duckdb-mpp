#pragma once

#include "common/endpoint.hpp"
#include "duckdb/common/index_vector.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class MppShardInfo;
class ShuffleManager;

class PhysicalShuffleWrite final : public PhysicalOperator {
public:
	explicit PhysicalShuffleWrite(vector<LogicalType> types, idx_t estimated_cardinality,
	                              ShuffleManager &shuffle_manager, TableCatalogEntry &table,
	                              physical_index_vector_t<idx_t> column_index_map,
	                              vector<unique_ptr<Expression>> bound_defaults, unique_ptr<Expression> hash_expression,
	                              vector<Endpoint> shard_locations, Endpoint local_endpoint);

	~PhysicalShuffleWrite() override;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface

	//! The sink method is called constantly with new input, as long as new input is available. Note that this method
	//! CAN be called in parallel, proper locking is needed when accessing dat
	//! a inside the GlobalSinkState.
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;

	//! The combine is called when a single thread has completed execution of its part of the pipeline, it is the final
	//! time that a specific LocalSinkState is accessible. This method can be called in parallel while other Sink() or
	//! Combine() calls are active on the same GlobalSinkState.
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;

	//! The finalize is called when ALL threads are finished execution. It is called only once per pipeline, and is
	//! entirely single threaded.
	//! If Finalize returns SinkResultType::Finished, the sink is marked as finished
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	OperatorPartitionInfo RequiredPartitionInfo() const override {
		return OperatorPartitionInfo::NoPartitionInfo();
	}

public:
	static void ResolveDefaults(const TableCatalogEntry &table, DataChunk &chunk,
	                            const physical_index_vector_t<idx_t> &column_index_map,
	                            ExpressionExecutor &defaults_executor, DataChunk &result);

public:
	ShuffleManager &shuffle_manager;

	//! The map from insert column index to table column index
	physical_index_vector_t<idx_t> column_index_map;
	//! The table to insert into
	optional_ptr<TableCatalogEntry> insert_table;
	//! The insert types
	vector<LogicalType> insert_types;
	//! The default expressions of the columns for which no value is provided
	vector<unique_ptr<Expression>> bound_defaults;
	unique_ptr<Expression> hash_expression;
	//! The endpoints of table shards
	vector<Endpoint> shard_locations;
	Endpoint local_endpoint;
	idx_t first_partition_id;
};

} // namespace duckdb