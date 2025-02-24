#include "mpp_client.hpp"

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include "duckdb/common/serializer/binary_deserializer.hpp"

#include "remote_query_result.hpp"
#include "common/endpoint.hpp"
#include "common/serializer.hpp"

using ::grpc::ClientContext;

namespace duckdb {

auto MppClient::NewClient(const std::string &server_addr) -> std::shared_ptr<MppClient> {
	// TODO: reuse channel and stub
	auto channel = grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials());
	auto stub = proto::MppService::NewStub(channel);
	return std::make_shared<MppClient>(std::move(stub), server_addr);
}

auto MppClient::NewClient(const Endpoint &server_addr) -> std::shared_ptr<MppClient> {
	return NewClient(server_addr.ToString());
}

auto MppClient::Query(std::string query, bool need_schema) -> std::unique_ptr<RemoteQueryResult> {
	auto context = std::make_shared<grpc::ClientContext>();
	auto request = proto::QueryRequest {};

	request.set_statement(query);

	auto reader = stub_->Query(context.get(), request);
	auto names = std::vector<std::string> {};
	auto types = std::vector<LogicalType> {};
	if (need_schema) {
		reader->WaitForInitialMetadata();

		auto it = context->GetServerInitialMetadata().find("custom-schema-bin");
		if (it == context->GetServerInitialMetadata().end()) {
			auto st = reader->Finish();
			auto msg = st.ok() ? "No schema in metadata" : st.error_message();
			throw Exception(ExceptionType::INVALID_INPUT, StringUtil::Format("%s: %s", context->peer(), msg));
		}
		auto schema_pb = proto::Schema {};
		if (!schema_pb.ParseFromArray(it->second.data(), (int)it->second.size())) {
			throw Exception(ExceptionType::INVALID_INPUT, "Invalid schema from swam server");
		}
		D_ASSERT(schema_pb.types_size() == schema_pb.names_size());

		types.reserve(schema_pb.types_size());
		names.reserve(schema_pb.names_size());

		for (auto &t : schema_pb.types()) {
			auto type = DeserializeFromString<LogicalType>(t);
			D_ASSERT(type.IsValid());
			types.emplace_back(type);
		}

		for (auto &name : *schema_pb.mutable_names()) {
			names.emplace_back(name);
		}
	}

	auto result = std::make_unique<RemoteQueryResult>(std::move(types), std::move(names), std::move(context),
	                                                  std::move(reader), shared_from_this());
	return result;
}

} // namespace duckdb