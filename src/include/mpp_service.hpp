#pragma once

#include "mpp.grpc.pb.h"

namespace duckdb {

class MppCatalog;

class MppServiceImpl final : public proto::MppService::Service {
public:
	explicit MppServiceImpl(MppCatalog &catalog) : catalog_(catalog) {
	}

	auto Query(::grpc::ServerContext *context, const proto::QueryRequest *request,
	           ::grpc::ServerWriter<proto::DataChunk> *writer) -> ::grpc::Status override;

	auto ShuffleRead(::grpc::ServerContext *context, const proto::ShuffleReadRequest *request,
	                 ::grpc::ServerWriter<proto::DataChunk> *writer) -> ::grpc::Status override;

private:
	MppCatalog &catalog_;
};

} // namespace duckdb
