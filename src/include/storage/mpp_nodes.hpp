#pragma once

#include "common/endpoint.hpp"
#include "duckdb/common/vector.hpp"
#include "storage/mpp_system_table.hpp"

#include <mutex>

namespace duckdb {

class ClientContext;
class MppCatalog;
class TableCatalogEntry;

class MppNodes : public MppSystemTable {
public:
	explicit MppNodes(MppCatalog &catalog) : MppSystemTable(catalog) {
	}

	static MppNodes &Get(MppCatalog &mpp_catalog);

	void Initialize(ClientContext &context);

	auto GetNodes(ClientContext &context) -> vector<Endpoint>;

	void AddNode(ClientContext &context, const Endpoint &node);

	void RemoveNode(ClientContext &context, const Endpoint &node);
};

} // namespace duckdb