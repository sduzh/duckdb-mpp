#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

typedef idx_t partition_id_t;

class MppCatalog;

enum class ShuffleManagerType : int {
	SIMPLE = 0,
};

class ShuffleManager {
public:
	ShuffleManager();

	ShuffleManager(const ShuffleManager &) = delete;
	void operator=(const ShuffleManager &) = delete;

	virtual ~ShuffleManager() = default;

	static unique_ptr<ShuffleManager> New(ShuffleManagerType type);

	static ShuffleManager &Get(MppCatalog &mpp_catalog);

	auto GetPartitionId(int32_t num_partitions) -> partition_id_t;

	virtual void OpenPartition(partition_id_t partition_id) = 0;

	virtual void BlockWrite(partition_id_t partition_id, string data) = 0;

	virtual auto BlockRead(partition_id_t partition_id) -> string = 0;

	virtual void ClosePartition(partition_id_t partition_id) = 0;

	virtual void RemotePartition(partition_id_t partition_id) = 0;

protected:
	atomic<partition_id_t> next_partition_id_;
};

} // namespace duckdb