#include "storage/mpp_catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "execution/mpp_physical_delete.hpp"
#include "function/mpp_table_scan.hpp"

namespace duckdb {
unique_ptr<PhysicalOperator> MppCatalog::PlanDelete(ClientContext &context, LogicalDelete &op,
                                                    unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		// TODO
		throw NotImplementedException("Does not support UPDATE with returning");
	}
	if (plan->type != PhysicalOperatorType::TABLE_SCAN) {
		throw NotImplementedException("Unsupported child of UPDATE: %s", EnumUtil::ToString(plan->type));
	}
	auto &scan = plan->Cast<PhysicalTableScan>();
	if (scan.function.name != MppTableScanFunction::NAME) {
		throw NotImplementedException("Unsupported table function: %s", scan.function.name);
	}
	auto &scan_data = scan.bind_data->Cast<MppTableScanBindData>();
	auto query_pattern = string("DELETE FROM %s");
	if (!scan_data.where_clause.empty()) {
		query_pattern += " WHERE ";
		query_pattern += scan_data.where_clause;
	}
	return make_uniq<MppPhysicalDelete>(op.types, op.estimated_cardinality, std::move(query_pattern), scan_data.shards,
	                                    op.return_chunk);
}
} // namespace duckdb
