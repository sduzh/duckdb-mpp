#pragma once

#include "duckdb/common/column_index.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {
// Try to extract a column index from a bound column ref expression, or a column ref recursively nested
// inside of a struct_extract call. If the expression is not a column ref (or nested column ref), return false.
inline bool TryGetBoundColumnIndex(const vector<ColumnIndex> &column_ids, const Expression &expr, ColumnIndex &result) {
	switch (expr.GetExpressionType()) {
	case ExpressionType::BOUND_COLUMN_REF: {
		auto &ref = expr.Cast<BoundColumnRefExpression>();
		result = column_ids[ref.binding.column_index];
		return true;
	}
	case ExpressionType::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.function.name == "struct_extract" || func.function.name == "struct_extract_at") {
			auto &child_expr = func.children[0];
			return TryGetBoundColumnIndex(column_ids, *child_expr, result);
		}
		return false;
	}
	default:
		return false;
	}
}

inline vector<BoundColumnRefExpression *> FindBoundColumnRefs(Expression &expr) {
	vector<BoundColumnRefExpression *> result;

	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		result.push_back(&expr.Cast<BoundColumnRefExpression>());
	}

	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
		auto child_refs = FindBoundColumnRefs(child);
		result.insert(result.end(), child_refs.begin(), child_refs.end());
	});

	return result;
}

inline optional_ptr<const BoundColumnRefExpression> GetColumnRef(const Expression &expr) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		return expr.Cast<BoundColumnRefExpression>();
	} else {
		return {};
	}
}

inline optional_ptr<const BoundConstantExpression> GetConstantExpression(const Expression &expr) {
	if (expr.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		return expr.Cast<BoundConstantExpression>();
	} else {
		return {};
	}
}

} // namespace duckdb