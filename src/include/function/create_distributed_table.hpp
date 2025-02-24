#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct CreateDistributedTableFunction {
	static TableFunction GetFunction();
};

} // namespace duckdb