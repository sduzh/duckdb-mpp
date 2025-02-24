#include "shuffle/simple_shuffle_manager.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

SimpleShuffleManager::SimpleShuffleManager() : ShuffleManager() {
}

SimpleShuffleManager::~SimpleShuffleManager() = default;

auto SimpleShuffleManager::GetChunkQueue(partition_id_t partition_id) -> shared_ptr<BlockingQueue<string>> {
	auto l = lock_guard(lock_);
	auto iter = partition_data_.find(partition_id);
	return (iter != partition_data_.end()) ? iter->second : nullptr;
}

void SimpleShuffleManager::OpenPartition(partition_id_t partition_id) {
	auto l = lock_guard(lock_);
	if (!partition_data_.contains(partition_id)) {
		partition_data_[partition_id] = make_shared_ptr<BlockingQueue<string>>(10);
	}
}

void SimpleShuffleManager::BlockWrite(partition_id_t partition_id, string data) {
	auto q = GetChunkQueue(partition_id);
	if (q) {
		q->Push(std::move(data));
	} else {
		throw InternalException("partition %d not opened or has been closed", partition_id);
	}
}

auto SimpleShuffleManager::BlockRead(partition_id_t partition_id) -> string {
	auto q = GetChunkQueue(partition_id);
	if (q) {
		auto value_or = q->Pop();
		if (value_or) {
			return std::move(value_or).value();
		} else {
			return {};
		}
	} else {
		throw InternalException("partition %d not opened or has been closed", partition_id);
	}
}

void SimpleShuffleManager::ClosePartition(partition_id_t partition_id) {
	auto q = GetChunkQueue(partition_id);
	if (q) {
		q->Close();
	} else {
		throw InternalException("partition %d not opened or has been removed", partition_id);
	}
}

void SimpleShuffleManager::RemotePartition(partition_id_t partition_id) {
	auto l = lock_guard(lock_);
	partition_data_.erase(partition_id);
}
} // namespace duckdb