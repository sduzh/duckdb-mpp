#pragma once

#include "duckdb/parser/column_definition.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

extern unique_ptr<Expression>
GetHashPartitionColumnExpression(ClientContext &context, const ColumnDefinition &partition_column, uint16_t buckets);

extern uint16_t GetShardId(ClientContext &context, const Value &input, uint16_t buckets);

} // namespace duckdb