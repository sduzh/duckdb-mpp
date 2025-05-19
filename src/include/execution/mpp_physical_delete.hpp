#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/bound_constraint.hpp"

namespace duckdb {

class MppShardInfo;

class MppPhysicalDelete : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

public:
	MppPhysicalDelete(vector<LogicalType> types, idx_t estimated_cardinality, string query_pattern,
	                  vector<MppShardInfo> shards, bool return_chunk);

	~MppPhysicalDelete() override;

	string query_pattern;
	vector<MppShardInfo> shards;
	bool return_chunk;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	// Source interface
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
