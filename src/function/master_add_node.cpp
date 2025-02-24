#include "function/master_add_node.hpp"

#include "storage/mpp_nodes.hpp"
#include "storage/mpp_catalog.hpp"

namespace duckdb {

namespace {

struct CreateDistributedTableBindData final : public TableFunctionData {
	string host;
	uint16_t port;

	CreateDistributedTableBindData(string h, uint16_t p) : host(std::move(h)), port(p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CreateDistributedTableBindData>(host, port);
	}

	bool Equals(const FunctionData &other) const override {
		auto rhs = other.Cast<CreateDistributedTableBindData>();
		return host == rhs.host && port == rhs.port;
	}
};

auto Bind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
          vector<string> &names) -> unique_ptr<FunctionData> {
	auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG);
	if (catalog.GetCatalogType() != "mpp") {
		throw BinderException("function master_add_node must be called in MPP database");
	}
	return_types.push_back(LogicalType::SQLNULL);
	names.push_back("result");
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("host cannot be NULL");
	}
	if (input.inputs[1].IsNull()) {
		throw InvalidInputException("port cannot be NULL");
	}
	auto host = input.inputs[0].ToString();
	auto port = input.inputs[1].GetValue<int32_t>();
	if (port < 0 || port > UINT16_MAX) {
		throw InvalidInputException("Invalid port number: %d", port);
	}
	return make_uniq<CreateDistributedTableBindData>(host, port);
}

void MasterAddNodeFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<CreateDistributedTableBindData>();
	auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG).Cast<MppCatalog>();
	MppNodes::Get(catalog).AddNode(context, Endpoint(bind_data.host, bind_data.port));
}
} // namespace

TableFunction MasterAddNodeFunction::GetFunction() {
	auto func = TableFunction();
	func.name = "master_add_node";
	func.arguments = {LogicalType::VARCHAR, LogicalType::INTEGER};
	func.function = MasterAddNodeFunc;
	func.bind = Bind;
	return func;
}
} // namespace duckdb
