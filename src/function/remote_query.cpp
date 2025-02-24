#include "function/remote_query.hpp"

#include <grpcpp/client_context.h>
#include <grpcpp/security/credentials.h>

#include "duckdb.hpp"
#include "remote_query_result.hpp"
#include "mpp_client.hpp"

namespace duckdb {
namespace {
class RemoteQueryFunctionData final : public TableFunctionData {
public:
	explicit RemoteQueryFunctionData(std::unique_ptr<RemoteQueryResult> r) : result(std::move(r)) {
	}

	std::unique_ptr<RemoteQueryResult> result;
};

unique_ptr<FunctionData> BindRemoteQuery(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(input.inputs.size() == 2);
	auto remote_addr = input.inputs[0].ToString();
	auto statement = input.inputs[1].ToString();
	auto client = MppClient::NewClient(remote_addr);
	auto result = client->Query(statement);

	return_types.assign(result->types().begin(), result->types().end());
	names.assign(result->names().begin(), result->names().end());

	return make_uniq_base<FunctionData, RemoteQueryFunctionData>(std::move(result));
}

void RemoteQueryFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RemoteQueryFunctionData>();
	auto &result = bind_data.result;
	auto chunk = result->Fetch();
	if (chunk) {
		output.Move(*chunk);
	} else if (result->HasError()) {
		throw Exception(ExceptionType::NETWORK, result->GetError());
	}
}
} // namespace

TableFunction RemoteQueryFunction::GetFunction() {
	auto func = TableFunction {};
	func.name = "remote_query";
	func.arguments = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	func.bind = BindRemoteQuery;
	func.function = RemoteQueryFunc;
	return func;
}
} // namespace duckdb
