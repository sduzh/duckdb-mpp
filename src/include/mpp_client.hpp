#pragma once

#include <memory>
#include <string>

#include <grpcpp/channel.h>

#include "duckdb.hpp"
#include "mpp.grpc.pb.h"

namespace duckdb {

class Endpoint;
class RemoteQueryResult;

class MppClient : public std::enable_shared_from_this<MppClient> {
public:
	explicit MppClient(std::shared_ptr<proto::MppService::Stub> stub, std::string server)
	    : stub_(std::move(stub)), server_addr_(std::move(server)) {
	}

	static auto NewClient(const std::string &server_addr) -> std::shared_ptr<MppClient>;

	static auto NewClient(const Endpoint &server_addr) -> std::shared_ptr<MppClient>;

	auto Query(std::string query, bool need_schema = true) -> std::unique_ptr<RemoteQueryResult>;

	auto GetServerAddr() const noexcept -> const std::string & {
		return server_addr_;
	}

private:
	std::shared_ptr<proto::MppService::Stub> stub_;
	std::string server_addr_;
};

} // namespace duckdb