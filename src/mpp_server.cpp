#include "mpp_server.hpp"

#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include "duckdb.hpp"
#include "mpp_service.hpp"
#include "storage/mpp_catalog.hpp"

namespace duckdb {

MppServer::MppServer(duckdb::MppCatalog &catalog) : catalog_(catalog) {
}

MppServer::~MppServer() = default;

void MppServer::StartServer(const Endpoint &server_addr) {
	D_ASSERT(!service_);
	service_ = make_uniq<MppServiceImpl>(catalog_);
	grpc::EnableDefaultHealthCheckService(true);
	// grpc::reflection::InitProtoReflectionServerBuilderPlugin();
	auto builder = ::grpc::ServerBuilder {};
	// Listen on the given address without any authentication mechanism.
	builder.AddListeningPort(server_addr.ToString(), grpc::InsecureServerCredentials());
	// Register "service" as the instance through which we'll communicate with
	// clients. In this case it corresponds to an *synchronous* service.
	builder.RegisterService(service_.get());
	// Finally assemble the server.
	server_.reset(builder.BuildAndStart().release());
}

void MppServer::StopServer() {
	if (server_) {
		server_->Shutdown();
		server_.reset();
		service_.reset();
	}
}

} // namespace duckdb
