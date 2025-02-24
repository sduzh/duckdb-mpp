#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct RemoteQueryFunction {
	static TableFunction GetFunction();
};

} // namespace duckdb