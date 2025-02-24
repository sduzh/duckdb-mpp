#pragma once

#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
class ColumnIndex;
class Expression;
class MppShardInfo;

extern void PruneShards(ClientContext &context, const Expression &filter, int32_t buckets,
                        vector<MppShardInfo> &shards);

} // namespace duckdb