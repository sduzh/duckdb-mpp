#pragma once

#include "duckdb/common/column_index.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
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

inline unique_ptr<Expression> RewriteExclusiveBetweenExpression(unique_ptr<Expression> expr) {
	if (expr->GetExpressionType() != ExpressionType::COMPARE_BETWEEN) {
		ExpressionIterator::EnumerateChildren(
		    *expr, [&](unique_ptr<Expression> &child) { child = RewriteExclusiveBetweenExpression(std::move(child)); });
		return expr;
	}
	auto &between = expr->Cast<BoundBetweenExpression>();
	if (between.lower_inclusive && between.upper_inclusive) {
		return expr;
	}
	unique_ptr<Expression> left;
	unique_ptr<Expression> right;
	if (between.lower_inclusive) {
		left = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, between.input->Copy(),
		                                            std::move(between.lower));
		right = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, between.input->Copy(),
		                                             std::move(between.upper));
	} else if (between.upper_inclusive) {
		left = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, between.input->Copy(),
		                                            std::move(between.lower));
		right = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO, between.input->Copy(),
		                                             std::move(between.upper));
	} else {
		left = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, between.input->Copy(),
		                                            std::move(between.lower));
		right = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, between.input->Copy(),
		                                             std::move(between.upper));
	}
	return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(left), std::move(right));
}

} // namespace duckdb