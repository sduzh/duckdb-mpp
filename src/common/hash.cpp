#include "common/hash.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

static unique_ptr<Expression> GetHashFunction(ClientContext &context, unique_ptr<Expression> child) {
	auto error = ErrorData {};
	auto func_binder = FunctionBinder(context);
	auto hash_children = vector<unique_ptr<Expression>> {};
	hash_children.emplace_back(std::move(child));
	auto hash_func = func_binder.BindScalarFunction(DEFAULT_SCHEMA, "hash", std::move(hash_children), error);
	if (!hash_func) {
		error.Throw();
	}
	return hash_func;
}

static unique_ptr<Expression> GetModuloFunction(ClientContext &context, unique_ptr<Expression> left,
                                                unique_ptr<Expression> right) {
	auto error = ErrorData {};
	auto func_binder = FunctionBinder(context);
	auto modulo_children = vector<unique_ptr<Expression>> {};
	modulo_children.resize(2);
	modulo_children[0] = std::move(left);
	modulo_children[1] = std::move(right);
	auto modulo_func = func_binder.BindScalarFunction(DEFAULT_SCHEMA, "%", std::move(modulo_children), error);
	if (!modulo_func) {
		error.Throw();
	}
	return modulo_func;
}

unique_ptr<Expression> GetHashPartitionColumnExpression(ClientContext &context,
                                                        const ColumnDefinition &partition_column, uint16_t buckets) {
	auto column_type = partition_column.GetType();
	auto column_ref = make_uniq<BoundReferenceExpression>(column_type, partition_column.Physical().index);
	// CAST(hash(col) % buckets AS USMALLINT)
	auto hash_expr = GetHashFunction(context, std::move(column_ref));
	auto buckets_expr = make_uniq<BoundConstantExpression>(Value::INTEGER(buckets));
	auto modulo = GetModuloFunction(context, std::move(hash_expr), std::move(buckets_expr));
	return BoundCastExpression::AddDefaultCastToType(std::move(modulo), LogicalType::USMALLINT);
}

uint16_t GetShardId(ClientContext &context, const Value &input, uint16_t buckets) {
	auto constant = make_uniq<BoundConstantExpression>(input);
	auto hash_func = GetHashFunction(context, std::move(constant));
	auto buckets_expr = make_uniq<BoundConstantExpression>(Value::INTEGER(buckets));
	auto modulo_func = GetModuloFunction(context, std::move(hash_func), std::move(buckets_expr));
	auto cast_expr = BoundCastExpression::AddDefaultCastToType(std::move(modulo_func), LogicalType::USMALLINT);
	auto executor = ExpressionExecutor(context);
	auto result = executor.EvaluateScalar(context, *cast_expr);
	D_ASSERT(!result.IsNull());
	return USmallIntValue::Get(result);
}

} // namespace duckdb
