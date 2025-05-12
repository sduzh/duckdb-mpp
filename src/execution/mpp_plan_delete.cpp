#include "storage/mpp_catalog.hpp"

namespace duckdb {
unique_ptr<PhysicalOperator> MppCatalog::PlanDelete(ClientContext &context, LogicalDelete &op,
                                                    unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("MppCatalog:PlanDelete");
}
} // namespace duckdb
