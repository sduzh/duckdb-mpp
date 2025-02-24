#pragma once

#include <memory>
#include <vector>

#include <grpcpp/client_context.h>

#include "duckdb.hpp"
#include "mpp.pb.h"
#include "mpp.grpc.pb.h"
#include "mpp_client.hpp"

namespace duckdb {

class MppClient;

class RemoteQueryResult {
public:
	explicit RemoteQueryResult(std::vector<LogicalType> types, std::vector<std::string> names,
	                           std::shared_ptr<grpc::ClientContext> context,
	                           std::unique_ptr<::grpc::ClientReader<proto::DataChunk>> reader,
	                           std::shared_ptr<MppClient> client)
	    : types_(std::move(types)), names_(std::move(names)), client_(std::move(client)), context_(std::move(context)),
	      reader_(std::move(reader)) {
	}

	auto types() -> const std::vector<LogicalType> & {
		return types_;
	}

	auto names() -> const std::vector<std::string> & {
		return names_;
	}

	auto Fetch() -> std::unique_ptr<DataChunk>;

	auto HasError() -> bool;

	auto GetError() -> std::string;

private:
	grpc::Status status_;
	std::vector<LogicalType> types_;
	std::vector<std::string> names_;
	std::shared_ptr<MppClient> client_;
	std::shared_ptr<grpc::ClientContext> context_;
	std::unique_ptr<::grpc::ClientReader<proto::DataChunk>> reader_;
};

} // namespace duckdb