#include "function/mpp_table_scan.hpp"

#include "common/distributed_table_info.hpp"
#include "common/expression_helper.hpp"
#include "common/hash.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/partition_info.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "function/prune_shards.hpp"
#include "mpp_client.hpp"
#include "remote_query_result.hpp"
#include "storage/mpp_catalog_utils.hpp"
#include "storage/mpp_shard_info.hpp"
#include "storage/mpp_table_entry.hpp"
#include "storage/mpp_shards.hpp"
#include "storage/mpp_tables.hpp"

namespace duckdb {
static void PruneShard(ClientContext &context, const LogicalGet &get, const Expression &filter,
                       MppTableScanBindData &bind_data);

static string FiltersToString(const vector<unique_ptr<Expression>> &filters) {
	if (filters.empty()) {
		return {};
	} else {
		string result = filters[0]->ToString();
		for (idx_t i = 1; i < filters.size(); i++) {
			result += " AND " + filters[i]->ToString();
		}
		return result;
	}
}

static void FilterShards(vector<MppShardInfo> &shards, uint16_t shard_id) {
	auto begin = shards.begin();
	auto end = shards.end();
	auto iter = std::remove_if(begin, end, [&](const auto &shard) { return shard.shard_id != shard_id; });
	shards.erase(iter, end);
}

static void FilterShards(vector<MppShardInfo> &shards, const set<uint16_t> &shard_ids) {
	auto iter = std::remove_if(shards.begin(), shards.end(),
	                           [&](const auto &shard) { return !shard_ids.contains(shard.shard_id); });
	shards.erase(iter, shards.end());
}

static bool IsPartitionColumnRef(const Expression &expr, const vector<ColumnIndex> &column_ids,
                                 const ColumnDefinition &partition_column) {
	auto col_ref = GetColumnRef(expr);
	if (!col_ref) {
		return false;
	}
	ColumnIndex index;
	if (!TryGetBoundColumnIndex(column_ids, *col_ref, index)) {
		return false;
	}
	if (index.GetPrimaryIndex() != partition_column.Logical().index || index.HasChildren()) {
		return false;
	}
	return true;
}

static void PruneShardsIn(ClientContext &context, const LogicalGet &get, const BoundOperatorExpression &filter,
                          MppTableScanBindData &bind_data) {
	D_ASSERT(filter.GetExpressionType() == ExpressionType::COMPARE_IN);
	if (!IsPartitionColumnRef(*filter.children[0], get.GetColumnIds(), *bind_data.partition_column)) {
		return;
	}
	auto selected_shards = set<uint16_t> {};
	for (idx_t i = 1, sz = filter.children.size(); i < sz; ++i) {
		auto child = GetConstantExpression(*filter.children[i]);
		if (!child) {
			return;
		}
		auto shard_id = GetShardId(context, child->value, bind_data.buckets);
		selected_shards.insert(shard_id);
	}
	FilterShards(bind_data.shards, selected_shards);
}

static void PruneShardsEq(ClientContext &context, const LogicalGet &get, const BoundComparisonExpression &comparison,
                          MppTableScanBindData &bind_data) {
	if (!IsPartitionColumnRef(*comparison.left, get.GetColumnIds(), *bind_data.partition_column)) {
		return;
	}
	auto right = GetConstantExpression(*comparison.right);
	if (!right) {
		return;
	}
	auto shard_id = GetShardId(context, right->value, bind_data.buckets);
	FilterShards(bind_data.shards, shard_id);
}

static void PruneShardsIsNull(ClientContext &context, const LogicalGet &get, const BoundOperatorExpression &op,
                              MppTableScanBindData &bind_data) {
	D_ASSERT(op.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL);
	auto left = GetColumnRef(*op.children[0]);
	if (!IsPartitionColumnRef(*op.children[0], get.GetColumnIds(), *bind_data.partition_column)) {
		return;
	}
	auto shard_id = GetShardId(context, Value(left->return_type), bind_data.buckets);
	FilterShards(bind_data.shards, shard_id);
}

static void PruneShardsOr(ClientContext &context, const LogicalGet &get, const BoundConjunctionExpression &conjunction,
                          MppTableScanBindData &bind_data) {
	D_ASSERT(conjunction.GetExpressionType() == ExpressionType::CONJUNCTION_OR);
	auto selected_shards = set<uint16_t> {};
	auto backup = bind_data.shards;
	for (auto &child : conjunction.children) {
		PruneShard(context, get, *child, bind_data);
		for (auto &shard : bind_data.shards) {
			selected_shards.insert(shard.shard_id);
		}
		bind_data.shards = backup;
	}
	FilterShards(bind_data.shards, selected_shards);
}

static void PruneShard(ClientContext &context, const LogicalGet &get, const Expression &filter,
                       MppTableScanBindData &bind_data) {
	switch (filter.GetExpressionType()) {
	case ExpressionType::COMPARE_EQUAL:
		PruneShardsEq(context, get, filter.Cast<BoundComparisonExpression>(), bind_data);
		break;
	case ExpressionType::COMPARE_IN:
		PruneShardsIn(context, get, filter.Cast<BoundOperatorExpression>(), bind_data);
		break;
	case ExpressionType::OPERATOR_IS_NULL:
		PruneShardsIsNull(context, get, filter.Cast<BoundOperatorExpression>(), bind_data);
		break;
	case ExpressionType::CONJUNCTION_OR:
		PruneShardsOr(context, get, filter.Cast<BoundConjunctionExpression>(), bind_data);
		break;
	default:
		break;
	}
}

MppTableScanBindData::MppTableScanBindData(ClientContext &context, TableCatalogEntry &table) : table(table) {
	auto table_info = DistributedTableInfo {};
	MppTables::Get(context).GetDistributedTableInfo(context, table.oid, table_info);
	partition_column = table.GetColumns().GetColumn(table_info.partition_column);
	buckets = table_info.buckets;
	MppShards::Get(context).GetShards(context, table.oid, shards);
}

MppTableScanBindData::MppTableScanBindData(TableCatalogEntry &table) : table(table) {
}

MppTableScanBindData::~MppTableScanBindData() = default;

bool MppTableScanBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<MppTableScanBindData>();
	return &other.table == &table;
}

unique_ptr<FunctionData> MppTableScanBindData::Copy() const {
	auto bind_data = make_uniq<MppTableScanBindData>(table);
	bind_data->column_ids = column_ids;
	bind_data->partition_column = partition_column;
	bind_data->buckets = buckets;
	bind_data->shards = shards;
	for (auto &e : filters) {
		bind_data->filters.emplace_back(e->Copy());
	}
	return std::move(bind_data);
}

struct MppTableScanLocalState : public LocalTableFunctionState {
	unique_ptr<RemoteQueryResult> remote_result;
};

class MppTableScanGlobalState : public GlobalTableFunctionState {
public:
	explicit MppTableScanGlobalState(const FunctionData *bind_data_p) {
		D_ASSERT(bind_data_p);
		auto &bind_data = bind_data_p->Cast<MppTableScanBindData>();
		shards = bind_data.shards;
		max_threads = shards.size();
		where_clause = bind_data.where_clause;
	}

	//! The mutex to protect concurrent accesses of shards
	mutex shards_lock;
	//! The shards of this table
	vector<MppShardInfo> shards;
	//! The maximum number of threads for this table scan.
	idx_t max_threads;
	//! The projected columns of this table scan.
	vector<string> projection_names;
	//! The where clause for remote query statement
	string where_clause;

public:
public:
	MppShardInfo GetNextScanShard() {
		lock_guard l(shards_lock);
		if (shards.empty()) {
			return {};
		} else {
			auto ret = shards.back();
			shards.pop_back();
			return ret;
		}
	}

	string RemoteQueryStatement(const MppShardInfo &shard) {
		D_ASSERT(!projection_names.empty());
		string s = "SELECT " + KeywordHelper::WriteOptionallyQuoted(projection_names[0]);
		for (idx_t i = 1; i < projection_names.size(); ++i) {
			s += ", ";
			s += KeywordHelper::WriteOptionallyQuoted(projection_names[i]);
		}
		s += " FROM ";
		s += ShardTableName<true>(shard.table_oid, shard.shard_id);
		if (!where_clause.empty()) {
			s += " WHERE ";
			s += where_clause;
		}
		return s;
	}

	unique_ptr<RemoteQueryResult> QueryNextShard() {
		auto shard = GetNextScanShard();
		if (!shard.IsValid()) {
			return nullptr;
		}
		auto client = MppClient::NewClient(shard.node);
		auto query = RemoteQueryStatement(shard);
		auto result = client->Query(query);
		return unique_ptr<RemoteQueryResult>(result.release());
	}

public:
	virtual unique_ptr<LocalTableFunctionState> InitLocalState(ExecutionContext &context,
	                                                           TableFunctionInitInput &input) = 0;
	virtual void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) = 0;
	virtual double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p) const = 0;
	virtual OperatorPartitionData TableScanGetPartitionData(ClientContext &context,
	                                                        TableFunctionGetPartitionInput &input) = 0;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

class MppTableScanState : public MppTableScanGlobalState {
public:
	explicit MppTableScanState(const FunctionData *bind_data_p) : MppTableScanGlobalState(bind_data_p) {
	}

public:
	unique_ptr<LocalTableFunctionState> InitLocalState(ExecutionContext &context,
	                                                   TableFunctionInitInput &input) override {
		auto lstate = make_uniq<MppTableScanLocalState>();
		lstate->remote_result = QueryNextShard();
		D_ASSERT(lstate->remote_result);
		return lstate;
	}

	void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) override {
		auto &lstate = data_p.local_state->Cast<MppTableScanLocalState>();
		do {
			auto data = lstate.remote_result->Fetch();
			if (data) {
				output.Move(*data);
				D_ASSERT(output.size() > 0);
				return;
			} else if (lstate.remote_result->HasError()) {
				throw ExecutorException(lstate.remote_result->GetError());
			}
			lstate.remote_result = QueryNextShard();
			if (!lstate.remote_result) {
				return;
			}
		} while (true);
	}

	double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p) const override {
		// TODO:
		return 0;
	}

	OperatorPartitionData TableScanGetPartitionData(ClientContext &context,
	                                                TableFunctionGetPartitionInput &input) override {
		// TODO
		return OperatorPartitionData(0);
	}
};

static void MppTableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<MppTableScanGlobalState>();
	gstate.TableScanFunc(context, data_p, output);
}

static unique_ptr<GlobalTableFunctionState> MppTableScanInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MppTableScanBindData>();
	auto gstate = make_uniq<MppTableScanState>(&bind_data);

	auto &mpp_table = bind_data.table.Cast<MppTableEntry>();
	if (input.projection_ids.empty()) {
		for (auto &col_idx : input.column_indexes) {
			D_ASSERT(!col_idx.IsRowIdColumn()); // FIXME
			auto &column = mpp_table.GetColumn(col_idx.ToLogical());
			gstate->projection_names.push_back(column.GetName());
		}
	} else {
		for (auto id : input.projection_ids) {
			auto &col_idx = input.column_indexes[id];
			D_ASSERT(!col_idx.IsRowIdColumn()); // FIXME
			auto &column = mpp_table.GetColumn(col_idx.ToLogical());
			gstate->projection_names.push_back(column.GetName());
		}
	}
	return gstate;
}

static unique_ptr<LocalTableFunctionState> MppTableScanInitLocal(ExecutionContext &context,
                                                                 TableFunctionInitInput &input,
                                                                 GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<MppTableScanGlobalState>();
	return gstate.InitLocalState(context, input);
}

static unique_ptr<BaseStatistics> MppTableScanStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                         column_t column_id) {
	// TODO
	return nullptr;
}

static unique_ptr<NodeStatistics> MppTableScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	// TODO:
	return nullptr;
}

static void PushFilter(ClientContext &context, LogicalGet &get, MppTableScanBindData &bind_data,
                       unique_ptr<Expression> &filter) {
	PruneShard(context, get, *filter, bind_data);
	bind_data.filters.emplace_back(std::move(filter));
}

static void MppComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                                     vector<unique_ptr<Expression>> &filters) {
	auto &data = bind_data->Cast<MppTableScanBindData>();
	for (auto &filter : filters) {
		PushFilter(context, get, data, filter);
	}
	data.where_clause = FiltersToString(data.filters);
	auto iter = std::remove_if(filters.begin(), filters.end(), [](auto &f) { return !f; });
	filters.erase(iter, filters.end());
}

static InsertionOrderPreservingMap<string> MppTableScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<MppTableScanBindData>();
	result["Table"] = bind_data.table.name;
	result["Shards"] = StringUtil::Format("%d/%d", bind_data.shards.size(), bind_data.buckets);
	result["Filter"] = bind_data.where_clause;
	return result;
}

static double MppTableScanProgress(ClientContext &context, const FunctionData *bind_data_p,
                                   const GlobalTableFunctionState *g_state_p) {
	auto &g_state = g_state_p->Cast<MppTableScanGlobalState>();
	return g_state.TableScanProgress(context, bind_data_p);
}

TableFunction MppTableScanFunction::GetFunction() {
	TableFunction scan_function(NAME, {}, MppTableScanFunc);
	scan_function.init_local = MppTableScanInitLocal;
	scan_function.init_global = MppTableScanInitGlobal;
	scan_function.statistics = MppTableScanStatistics;
	// scan_function.dependency = TableScanDependency;
	scan_function.cardinality = MppTableScanCardinality;
	scan_function.pushdown_complex_filter = MppComplexFilterPushdown;
	scan_function.to_string = MppTableScanToString;
	scan_function.table_scan_progress = MppTableScanProgress;
	// scan_function.get_partition_data = TableScanGetPartitionData;
	// scan_function.get_partition_stats = TableScanGetPartitionStats;
	// scan_function.get_bind_info = TableScanGetBindInfo;
	scan_function.projection_pushdown = true;
	scan_function.filter_pushdown = true;
	scan_function.filter_prune = true;
	// scan_function.sampling_pushdown = true;
	// scan_function.serialize = TableScanSerialize;
	// scan_function.deserialize = TableScanDeserialize;
	return scan_function;
}

} // namespace duckdb