#include <grpcpp/create_channel.h>
#include "common/constants.hpp"
#include "common/serializer.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "mpp_client.hpp"
#include "function/shuffle_read.hpp"

namespace duckdb {

struct ShuffleReadBindData : public FunctionData {
	string remote_addr;
	idx_t table_oid;
	idx_t partition_id;

	unique_ptr<FunctionData> Copy() const override {
		auto new_data = make_uniq<ShuffleReadBindData>();
		new_data->remote_addr = remote_addr;
		new_data->table_oid = table_oid;
		new_data->partition_id = partition_id;
		return new_data;
	}

	bool Equals(const FunctionData &other) const override {
		auto &rhs = other.Cast<ShuffleReadBindData>();
		return rhs.remote_addr == remote_addr && rhs.partition_id == partition_id && rhs.table_oid == table_oid;
	}
};

struct ShuffleReadGlobalState : public GlobalTableFunctionState {
	std::shared_ptr<proto::MppService::Stub> stub_;
};

struct ShuffleReadLocalState : public LocalTableFunctionState {
	grpc::ClientContext client_context;
	proto::ShuffleReadRequest request;
	std::unique_ptr<::grpc::ClientReader<proto::DataChunk>> reader;
};

unique_ptr<FunctionData> BindShuffleRead(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("first argument of shuffle_read() function cannot be NULL");
	}
	if (input.inputs[1].IsNull()) {
		throw BinderException("second argument of shuffle_read() function cannot be NULL");
	}
	if (input.inputs[2].IsNull()) {
		throw BinderException("third argument of shuffle_read() function cannot be NULL");
	}
	auto data = make_uniq<ShuffleReadBindData>();
	data->remote_addr = input.inputs[0].ToString();
	data->partition_id = input.inputs[1].GetValue<int64_t>();
	auto shard_table_name = input.inputs[2].ToString();
	auto &shard_table = Catalog::GetEntry<TableCatalogEntry>(context, "", MPP_SHARDS_SCHEMA, shard_table_name);
	return_types = shard_table.GetTypes();
	names = shard_table.GetColumns().GetColumnNames();
	return data;
}

unique_ptr<GlobalTableFunctionState> ShuffleReadGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &data = input.bind_data->Cast<ShuffleReadBindData>();
	auto gstate = make_uniq<ShuffleReadGlobalState>();
	auto channel = grpc::CreateChannel(data.remote_addr, grpc::InsecureChannelCredentials());
	gstate->stub_ = proto::MppService::NewStub(channel);
	return gstate;
}

unique_ptr<LocalTableFunctionState> ShuffleReadLocalInit(ExecutionContext &context, TableFunctionInitInput &input,
                                                         GlobalTableFunctionState *global_state) {
	auto &data = input.bind_data->Cast<ShuffleReadBindData>();
	auto gstate = global_state->Cast<ShuffleReadGlobalState>();
	auto lstate = make_uniq<ShuffleReadLocalState>();
	lstate->request.set_partition_id(data.partition_id);
	lstate->reader = gstate.stub_->ShuffleRead(&lstate->client_context, lstate->request);
	return lstate;
}

void ShuffleReadFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &lstate = data.local_state->Cast<ShuffleReadLocalState>();
	auto &reader = lstate.reader;
	auto chunk_pb = proto::DataChunk {};
	if (reader->Read(&chunk_pb)) {
		auto ptr = DeserializeFromString<DataChunk>(chunk_pb.chunk());
		output.Move(*ptr);
	} else {
		auto status = reader->Finish();
		lstate.client_context.TryCancel();
		if (!status.ok()) {
			throw ExecutorException(status.error_message());
		}
	}
}

TableFunction ShuffleReadFunction::GetFunction() {
	auto func = TableFunction();
	func.name = "shuffle_read";
	func.arguments = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR};
	func.function = ShuffleReadFunc;
	func.bind = BindShuffleRead;
	func.init_global = ShuffleReadGlobalInit;
	func.init_local = ShuffleReadLocalInit;
	return func;
}

} // namespace duckdb