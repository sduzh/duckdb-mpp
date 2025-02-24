#pragma once

#include "duckdb.hpp"
#include "common/endpoint.hpp"

namespace grpc {
class Server;
}

namespace duckdb {

class MppCatalog;
class MppServiceImpl;

class MppServer {
public:
	explicit MppServer(MppCatalog &catalog);

	~MppServer();

	void StartServer(const Endpoint &server_addr);

	void StopServer();

	MppCatalog &GetCatalog() {
		return catalog_;
	}

private:
	MppCatalog &catalog_;
	unique_ptr<MppServiceImpl> service_;
	unique_ptr<::grpc::Server> server_;
};

} // namespace duckdb
