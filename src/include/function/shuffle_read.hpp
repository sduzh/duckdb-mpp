#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct ShuffleReadFunction {
	static TableFunction GetFunction();
};

} // namespace duckdb