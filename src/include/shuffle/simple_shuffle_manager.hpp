#pragma once

#include "common/blocking_queue.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "shuffle/shuffle_manager.hpp"

namespace duckdb {

class SimpleShuffleManager : public ShuffleManager {
public:
	SimpleShuffleManager();

	~SimpleShuffleManager() override;

	void OpenPartition(partition_id_t partition_id) override;

	void BlockWrite(partition_id_t partition_id, string data) override;

	auto BlockRead(partition_id_t partition_id) -> string override;

	void ClosePartition(partition_id_t partition_id) override;

	void RemotePartition(partition_id_t partition_id) override;

private:
	auto GetChunkQueue(partition_id_t partition_id) -> shared_ptr<BlockingQueue<string>>;

	mutable mutex lock_;
	unordered_map<partition_id_t, shared_ptr<BlockingQueue<string>>> partition_data_;
};

} // namespace duckdb