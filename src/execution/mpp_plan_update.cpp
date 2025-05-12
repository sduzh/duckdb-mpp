#include "storage/mpp_catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
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
		throw NotImplementedException("Unsupported plan type: %d", plan->type);
	}
	auto &proj = plan->Cast<PhysicalProjection>();
	if (proj.children[0]->type != PhysicalOperatorType::TABLE_SCAN) {
		throw NotImplementedException("Unsupported plan type: %d", proj.children[0]->type);
	}
	auto &scan = proj.children[0]->Cast<PhysicalTableScan>();
	if (scan.function.name != MppTableScanFunction::GetFunction().name) {
		throw NotImplementedException("Unsupported function: %s", scan.function.name);
	}
	auto &scan_data = scan.bind_data->Cast<MppTableScanBindData>();
	auto query_pattern = string("UPDATE %s SET ");
	D_ASSERT(op.columns.size() == proj.select_list.size() - 1);
	auto &table = op.table;
	for (idx_t i = 0; i < op.columns.size(); ++i) {
		query_pattern += KeywordHelper::WriteOptionallyQuoted(table.GetColumns().GetColumn(op.columns[i]).GetName());
		query_pattern += "=";
		query_pattern += proj.select_list[i]->ToString();
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
