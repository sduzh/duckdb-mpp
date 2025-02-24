#include "shuffle/shuffle_manager.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "shuffle/simple_shuffle_manager.hpp"
#include "storage/mpp_catalog.hpp"

namespace duckdb {
ShuffleManager::ShuffleManager() {
	next_partition_id_ = (partition_id_t)Timestamp::GetCurrentTimestamp().value;
}

auto ShuffleManager::GetPartitionId(int32_t num_partitions) -> partition_id_t {
	return next_partition_id_.fetch_add(num_partitions, std::memory_order_relaxed);
}

unique_ptr<ShuffleManager> ShuffleManager::New(ShuffleManagerType type) {
	if (type == ShuffleManagerType::SIMPLE) {
		return make_uniq<SimpleShuffleManager>();
	} else {
		throw NotImplementedException("ShuffleManager::New");
	}
}

ShuffleManager &ShuffleManager::Get(MppCatalog &mpp_catalog) {
	return *mpp_catalog.GetShuffleManager();
}
} // namespace duckdb
