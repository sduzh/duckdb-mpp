#include "storage/mpp_catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "execution/mpp_physical_update.hpp"
#include "function/mpp_table_scan.hpp"

namespace duckdb {
unique_ptr<PhysicalOperator> MppCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                    unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		// TODO
		throw NotImplementedException("Does not support UPDATE with returning");
	}
	if (plan->type != PhysicalOperatorType::PROJECTION) {
		throw NotImplementedException("Unsupported child of UPDATE: %s", EnumUtil::ToString(plan->type));
	}
	auto &proj = plan->Cast<PhysicalProjection>();
	if (proj.children[0]->type != PhysicalOperatorType::TABLE_SCAN) {
		throw NotImplementedException("Unsupported child of PROJECTION: %s",
		                              EnumUtil::ToString(proj.children[0]->type));
	}
	auto &scan = proj.children[0]->Cast<PhysicalTableScan>();
	if (scan.function.name != MppTableScanFunction::NAME) {
		throw NotImplementedException("Unsupported table function: %s", scan.function.name);
	}
	auto &scan_data = scan.bind_data->Cast<MppTableScanBindData>();
	auto query_pattern = string("UPDATE %s SET ");
	D_ASSERT(op.columns.size() == op.expressions.size());
	auto &table = op.table;
	for (idx_t i = 0; i < op.columns.size(); ++i) {
		query_pattern += KeywordHelper::WriteOptionallyQuoted(table.GetColumns().GetColumn(op.columns[i]).GetName());
		query_pattern += "=";

		auto &expr = op.expressions[i];
		if (expr->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			query_pattern += "DEFAULT";
		} else {
			D_ASSERT(expr->GetExpressionType() == ExpressionType::BOUND_REF);
			auto idx = expr->Cast<BoundReferenceExpression>().index;
			D_ASSERT(idx < proj.select_list.size());
			query_pattern += proj.select_list[idx]->ToString();
		}
		query_pattern += (i + 1 == op.columns.size()) ? " " : ", ";
	}
	if (!scan_data.where_clause.empty()) {
		query_pattern += "WHERE ";
		query_pattern += scan_data.where_clause;
	}
	return make_uniq<MppPhysicalUpdate>(op.types, op.estimated_cardinality, std::move(query_pattern), scan_data.shards,
	                                    op.return_chunk);
}
} // namespace duckdb
