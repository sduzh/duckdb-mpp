#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {
struct MasterAddNodeFunction {
	static TableFunction GetFunction();
};
} // namespace duckdb