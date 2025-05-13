#include "execution/mpp_physical_update.hpp"

#include "mpp_client.hpp"
#include "remote_query_result.hpp"
#include "storage/mpp_catalog_utils.hpp"
#include "storage/mpp_shard_info.hpp"

namespace duckdb {

MppPhysicalUpdate::MppPhysicalUpdate(vector<LogicalType> types, idx_t estimated_cardinality, std::string query_pattern,
                                     vector<MppShardInfo> shards, bool return_chunk)
    : PhysicalOperator(TYPE, std::move(types), estimated_cardinality), query_pattern(std::move(query_pattern)),
      shards(std::move(shards)), return_chunk(return_chunk) {
}

MppPhysicalUpdate::~MppPhysicalUpdate() = default;

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class MppUpdateGlobalState : public GlobalSourceState {
public:
	explicit MppUpdateGlobalState(const vector<MppShardInfo> &shards, bool return_chunk)
	    : shards(shards), next_shard(0), updated_count(0), finished_shards(0) {
	}

	idx_t MaxThreads() override {
		return shards.size();
	}

	bool RemoveShard(MppShardInfo &info) {
		lock_guard l(lock);
		if (next_shard < shards.size()) {
			info = shards[next_shard++];
			return true;
		} else {
			return false;
		}
	}

	bool FinishShard(idx_t count) {
		lock_guard l(lock);
		updated_count += count;
		++finished_shards;
		D_ASSERT(finished_shards <= shards.size());
		return finished_shards == shards.size();
	}

	idx_t GetUpdatedCount() {
		lock_guard l(lock);
		return updated_count;
	}

	mutex lock;
	const vector<MppShardInfo> &shards;
	idx_t next_shard;
	idx_t updated_count;
	idx_t finished_shards;
};

class MppUpdateLocalState : public LocalSourceState {
public:
	MppUpdateLocalState() {
	}

	string query;
	std::unique_ptr<RemoteQueryResult> query_result;
};

unique_ptr<GlobalSourceState> MppPhysicalUpdate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<MppUpdateGlobalState>(shards, return_chunk);
}

unique_ptr<LocalSourceState> MppPhysicalUpdate::GetLocalSourceState(ExecutionContext &context,
                                                                    GlobalSourceState &gstate) const {
	return make_uniq<MppUpdateLocalState>();
}

SourceResultType MppPhysicalUpdate::GetData(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<MppUpdateGlobalState>();
	auto &lstate = input.local_state.Cast<MppUpdateLocalState>();
	do {
		if (!lstate.query_result) {
			MppShardInfo shard;
			if (gstate.RemoveShard(shard)) {
				auto remote_table_name = ShardTableName<true>(shard.table_oid, shard.shard_id);
				auto query = StringUtil::Format(query_pattern, remote_table_name);
				lstate.query = query;
				lstate.query_result = MppClient::NewClient(shard.node)->Query(query, false);
			} else {
				return SourceResultType::FINISHED;
			}
		}
		// TODO: non-block read?
		auto data = lstate.query_result->Fetch();
		if (data) {
			if (return_chunk) {
				chunk.Move(*data);
				D_ASSERT(chunk.size() > 0);
				return SourceResultType::HAVE_MORE_OUTPUT;
			}
			// else
			D_ASSERT(data->ColumnCount() == 1);
			D_ASSERT(data->GetTypes()[0].id() == LogicalTypeId::BIGINT);
			D_ASSERT(data->size() == 1);
			auto updated_count = BigIntValue::Get(data->GetValue(0, 0));
			if (gstate.FinishShard(updated_count)) {
				chunk.SetCardinality(1);
				chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.GetUpdatedCount())));
				return SourceResultType::HAVE_MORE_OUTPUT;
			}
		} else if (lstate.query_result->HasError()) {
			throw ExecutorException(lstate.query + ": " + lstate.query_result->GetError());
		} else {
			lstate.query_result.reset();
		}
	} while (true);
}

} // namespace duckdb