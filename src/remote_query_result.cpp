#include "remote_query_result.hpp"
#include "common/serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

namespace duckdb {

auto RemoteQueryResult::Fetch() -> std::unique_ptr<DataChunk> {
	auto chunk_pb = proto::DataChunk {};
	if (reader_->Read(&chunk_pb)) {
		auto ptr = DeserializeFromString<DataChunk>(chunk_pb.chunk());
		return std::unique_ptr<DataChunk>(ptr.release());
	} else {
		status_ = reader_->Finish();
		context_->TryCancel();
		return nullptr;
	}
}

auto RemoteQueryResult::HasError() -> bool {
	return !status_.ok();
}

auto RemoteQueryResult::GetError() -> std::string {
	return status_.error_message();
}

} // namespace duckdb