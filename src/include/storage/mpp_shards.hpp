#pragma once

#include "duckdb/common/vector.hpp"
#include "mpp_shard_info.hpp"
#include "storage/mpp_system_table.hpp"

#include <mutex>

namespace duckdb {

class ClientContext;
class MppCatalog;
class TableCatalogEntry;

class MppShards : public MppSystemTable {
public:
	explicit MppShards(MppCatalog &catalog) : MppSystemTable(catalog) {
	}

	static MppShards &Get(ClientContext &context);

	static MppShards &Get(MppCatalog &mpp_catalog);

	void Initialize(ClientContext &context);

	void AddShards(ClientContext &context, const vector<MppShardInfo> &shards);

	void GetShards(ClientContext &context, idx_t table_oid, vector<MppShardInfo> &shards);

	void UpdateShard(ClientContext &context, idx_t table_oid, idx_t shard_id, const MppShardInfo &shard);

	void DeleteShards(ClientContext &context, idx_t table_oid);

private:
	vector<StorageIndex> column_ids_;
};

} // namespace duckdb