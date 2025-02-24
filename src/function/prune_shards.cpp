#include "function/prune_shards.hpp"

#include <algorithm>

#include "common/hash.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "storage/mpp_shard_info.hpp"

namespace duckdb {

template <bool REVERSE_SELECT>
static void FilterShards(vector<MppShardInfo> &shards, uint16_t shard_id) {
	if constexpr (!REVERSE_SELECT) {
		auto iter = std::remove_if(shards.begin(), shards.end(),
		                           [&](const MppShardInfo &shard) { return shard.shard_id != shard_id; });
		shards.erase(iter, shards.end());
	} else {
		auto iter = std::remove_if(shards.begin(), shards.end(),
		                           [&](const MppShardInfo &shard) { return shard.shard_id == shard_id; });
		shards.erase(iter, shards.end());
	}
}

template <bool REVERSE_SELECT>
static void FilterShards(vector<MppShardInfo> &shards, const set<uint16_t> &shard_ids) {
	if constexpr (!REVERSE_SELECT) {
		auto iter = std::remove_if(shards.begin(), shards.end(),
		                           [&](const MppShardInfo &shard) { return !shard_ids.contains(shard.shard_id); });
		shards.erase(iter, shards.end());
	} else {
		auto iter = std::remove_if(shards.begin(), shards.end(),
		                           [&](const MppShardInfo &shard) { return shard_ids.contains(shard.shard_id); });
		shards.erase(iter, shards.end());
	}
}

optional_ptr<const BoundColumnRefExpression> GetColumnRef(const Expression &expr) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		return expr.Cast<BoundColumnRefExpression>();
	} else {
		return {};
	}
}

optional_ptr<const BoundConstantExpression> GetConstantExpression(const Expression &expr) {
	if (expr.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		return expr.Cast<BoundConstantExpression>();
	} else {
		return {};
	}
}

void PruneShardsIn(ClientContext &context, const BoundOperatorExpression &filter, int32_t buckets,
                   vector<MppShardInfo> &shards) {
	D_ASSERT(filter.GetExpressionType() == ExpressionType::COMPARE_IN);
	auto left = GetColumnRef(*filter.children[0]);
	if (!left) {
		return;
	}
	auto selected_shards = set<uint16_t> {};
	for (idx_t i = 1, sz = filter.children.size(); i < sz; ++i) {
		auto child = GetConstantExpression(*filter.children[i]);
		if (!child) {
			return;
		}
		auto shard_id = GetShardId(context, child->value, buckets);
		selected_shards.insert(shard_id);
	}
	FilterShards<false>(shards, selected_shards);
}

void PruneShardsEq(ClientContext &context, const BoundComparisonExpression &comparison, int32_t buckets,
                   vector<MppShardInfo> &shards) {
	auto left = GetColumnRef(*comparison.left);
	auto right = GetConstantExpression(*comparison.right);
	if (!left || !right) {
		return;
	}
	auto shard_id = GetShardId(context, right->value, buckets);
	FilterShards<false>(shards, shard_id);
}

void PruneShardsIsNull(ClientContext &context, const BoundOperatorExpression &op, int32_t buckets,
                       vector<MppShardInfo> &shards) {
	D_ASSERT(op.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL);
	auto left = GetColumnRef(*op.children[0]);
	if (!left) {
		return;
	}
	auto shard_id = GetShardId(context, Value(left->return_type), buckets);
	FilterShards<false>(shards, shard_id);
}

void PruneShardsOr(ClientContext &context, const BoundConjunctionExpression &conjunction, int32_t buckets,
                   vector<MppShardInfo> &shards) {
	D_ASSERT(conjunction.GetExpressionType() == ExpressionType::CONJUNCTION_OR);
	auto selected_shards = set<uint16_t> {};
	for (auto &child : conjunction.children) {
		auto copy = shards;
		PruneShards(context, *child, buckets, copy);
		for (auto &shard : copy) {
			selected_shards.insert(shard.shard_id);
		}
	}
	FilterShards<false>(shards, selected_shards);
}

void PruneShards(ClientContext &context, const Expression &filter, int32_t buckets, vector<MppShardInfo> &shards) {
	switch (filter.GetExpressionType()) {
	case ExpressionType::COMPARE_EQUAL:
		PruneShardsEq(context, filter.Cast<BoundComparisonExpression>(), buckets, shards);
		break;
	case ExpressionType::COMPARE_IN:
		PruneShardsIn(context, filter.Cast<BoundOperatorExpression>(), buckets, shards);
		break;
	case ExpressionType::OPERATOR_IS_NULL:
		PruneShardsIsNull(context, filter.Cast<BoundOperatorExpression>(), buckets, shards);
		break;
	case ExpressionType::CONJUNCTION_OR:
		PruneShardsOr(context, filter.Cast<BoundConjunctionExpression>(), buckets, shards);
		break;
	default:
		break;
	}
}

} // namespace duckdb