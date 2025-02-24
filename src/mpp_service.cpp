#include "mpp_service.hpp"
#include "duckdb.hpp"
#include "duckdb/main/client_data.hpp"
#include "common/serializer.hpp"
#include "shuffle/shuffle_manager.hpp"
#include "storage/mpp_catalog.hpp"

namespace duckdb {

using namespace ::grpc;

inline auto ConvertResultSchemaToProtobuf(vector<LogicalType> &types, vector<string> &names)
    -> unique_ptr<proto::Schema> {
	D_ASSERT(types.size() == names.size());
	auto schema = make_uniq<proto::Schema>();
	for (auto &type : types) {
		D_ASSERT(type.IsValid());
		schema->add_types(SerializeToString(type));
		auto t = DeserializeFromString<LogicalType>(SerializeToString(type));
		D_ASSERT(t.id() == type.id());
	}
	for (auto &name : names) {
		schema->add_names(name);
	}
	return schema;
}

auto MppServiceImpl::Query(ServerContext *context, const proto::QueryRequest *request,
                           ServerWriter<proto::DataChunk> *writer) -> Status {
	auto conn = Connection(catalog_.GetDatabase());
	conn.Query("USE " + catalog_.GetBase().GetName())->Fetch();
	auto result = conn.SendQuery(request->statement());
	auto chunk = unique_ptr<DataChunk> {};
	if (result->HasError()) {
		return Status(StatusCode::INVALID_ARGUMENT, result->GetError());
	}

	auto schema = ConvertResultSchemaToProtobuf(result->types, result->names);
	auto metadata = schema->SerializeAsString();
	context->AddInitialMetadata("custom-schema-bin", schema->SerializeAsString());

	while (true) {
		auto error = ErrorData {};
		if (!result->TryFetch(chunk, error)) { // error
			return Status(StatusCode::INVALID_ARGUMENT, error.Message());
		} else if (chunk == nullptr) { // no more result
			return Status::OK;
		} else {
			auto chunk_pb = proto::DataChunk {};
			chunk_pb.set_chunk(SerializeToString(*chunk));
			writer->Write(chunk_pb);
		}
	}
}
auto MppServiceImpl::ShuffleRead(::grpc::ServerContext *context, const proto::ShuffleReadRequest *request,
                                 ::grpc::ServerWriter<proto::DataChunk> *writer) -> ::grpc::Status {
	auto &shuffle_manager = ShuffleManager::Get(catalog_);
	try {
		while (true) {
			auto data = shuffle_manager.BlockRead(request->partition_id());
			if (data.empty()) {
				return Status::OK;
			}
			auto chunk_pb = proto::DataChunk {};
			chunk_pb.mutable_chunk()->swap(data);
			writer->Write(chunk_pb);
		}
	} catch (const std::exception &e) {
		return Status(StatusCode::INTERNAL, e.what());
	}
}
} // namespace duckdb