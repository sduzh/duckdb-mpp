#include "function/create_distributed_table.hpp"

#include "common/constants.hpp"
#include "common/endpoint.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "mpp_client.hpp"
#include "storage/mpp_nodes.hpp"
#include "remote_query_result.hpp"
#include "common/distributed_table_info.hpp"
#include "storage/mpp_catalog_utils.hpp"
#include "storage/mpp_shards.hpp"
#include "storage/mpp_catalog.hpp"
#include "storage/mpp_tables.hpp"
#include "storage/mpp_transaction.hpp"

namespace duckdb {
namespace {

struct CreateDistributedTableBindData final : public FunctionData {
	unique_ptr<CreateTableInfo> create_info;
	PhysicalIndex partition_column {DConstants::INVALID_INDEX};
	uint16_t bucket_count;

	bool Equals(const FunctionData &other) const override {
		auto &rhs = other.Cast<CreateDistributedTableBindData>();
		return create_info->ToString() == rhs.create_info->ToString() && partition_column == rhs.partition_column &&
		       bucket_count == rhs.bucket_count;
	}

	unique_ptr<FunctionData> Copy() const override {
		auto new_data = make_uniq<CreateDistributedTableBindData>();
		new_data->create_info = unique_ptr_cast<CreateInfo, CreateTableInfo>(create_info->Copy());
		new_data->partition_column = partition_column;
		new_data->bucket_count = bucket_count;
		return new_data;
	}
};

unique_ptr<CreateTableInfo> ParseCreateTableStatement(const string &statement) {
	auto parser = Parser();
	parser.ParseQuery(statement);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT) {
		throw SyntaxException("Not a distributed table creation statement");
	}
	auto &create_statement = parser.statements[0]->Cast<CreateStatement>();
	if (create_statement.info->type != CatalogType::TABLE_ENTRY) {
		throw SyntaxException("Not a distributed table creation statement");
	}
	auto create_table_info = unique_ptr_cast<CreateInfo, CreateTableInfo>(std::move(create_statement.info));
	if (create_table_info->temporary || create_table_info->internal || create_table_info->query) {
		throw SyntaxException("Not a distributed table creation statement");
	}
	return create_table_info;
}

unique_ptr<FunctionData> BindCreateDistributedTable(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("result");
	return_types.push_back(LogicalType::VARCHAR);
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("create table statement cannot be NULL");
	}
	if (input.inputs[1].IsNull()) {
		throw InvalidInputException("partition column name cannot be NULL");
	}
	if (input.inputs[2].IsNull()) {
		throw InvalidInputException("bucket count cannot be NULL");
	}

	auto create_info = ParseCreateTableStatement(StringValue::Get(input.inputs[0]));
	auto partition_column = StringValue::Get(input.inputs[1]);
	auto buckets = USmallIntValue::Get(input.inputs[2].DefaultCastAs(LogicalType::USMALLINT, true));
	if (buckets <= 0) {
		throw InvalidInputException("buckets must be positive: %d", buckets);
	}
	if (!create_info->columns.ColumnExists(partition_column)) {
		throw BinderException("Partition column '%s' does not exist in table", partition_column);
	}
	auto data = make_uniq<CreateDistributedTableBindData>();
	data->partition_column = create_info->columns.GetColumn(partition_column).Physical();
	data->bucket_count = buckets;
	data->create_info = std::move(create_info);
	return data;
}

string GetCreateShardSQL(TableCatalogEntry &target_table, const vector<MppShardInfo> &shards) {
	auto oid = target_table.oid;
	auto first_table_name = string {};
	auto s = string {};
	s += "BEGIN;";
	s += "CREATE SCHEMA IF NOT EXISTS ";
	s += MPP_SHARDS_SCHEMA;
	s += ";";
	for (size_t i = 0, sz = shards.size(); i < sz; ++i) {
		auto shard_id = shards[i].shard_id;
		auto shard_table_name = ShardTableName<true>(oid, shard_id);
		if (i == 0) {
			s += StringUtil::Format("CREATE OR REPLACE TABLE %s", shard_table_name);
			s += TableCatalogEntry::ColumnsToSQL(target_table.GetColumns(), target_table.GetConstraints()) + ";";
			first_table_name = shard_table_name;
		} else {
			s += StringUtil::Format("CREATE OR REPLACE TABLE %s AS TABLE %s;", shard_table_name, first_table_name);
		}
	}
	s += "COMMIT;";
	return s;
}

void CreateDistributedTableFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<CreateDistributedTableBindData>();
	auto &catalog = Catalog::GetCatalog(context, data.create_info->catalog).Cast<MppCatalog>();
	auto &transaction = MppTransaction::Get(context, catalog);
	auto nodes = MppNodes::Get(catalog).GetNodes(transaction.GetDuckContext());
	if (nodes.empty()) {
		throw InternalException("Empty cluster");
	}

	auto &schema = catalog.GetSchema(context, data.create_info->schema);
	auto bound_create = BoundCreateTableInfo(schema, data.create_info->Copy());
	auto entry = catalog.CreateTable(transaction.GetDuckContext(), bound_create);
	if (!entry) {
		D_ASSERT(data.create_info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
		return;
	}
	auto &table_ref = entry->Cast<TableCatalogEntry>();
	auto node_shards = map<string, vector<MppShardInfo>> {};
	auto start_node_idx = RandomEngine::Get(context).NextRandomInteger(0, nodes.size() - 1);
	for (uint16_t i = 0; i < data.bucket_count; ++i) {
		auto &node = nodes[(start_node_idx + i) % nodes.size()];
		auto info = MppShardInfo {.table_oid = table_ref.oid, .shard_id = i, .node = node.ToString()};
		node_shards[node.ToString()].emplace_back(std::move(info));
	}

	auto results = vector<std::unique_ptr<RemoteQueryResult>> {};
	for (auto &[node, shards] : node_shards) {
		auto client = MppClient::NewClient(node);
		auto query = GetCreateShardSQL(table_ref, shards);
		auto result = client->Query(query);
		results.emplace_back(std::move(result));
	}

	for (auto &result : results) {
		(void)result->Fetch();
		if (result->HasError()) {
			throw InternalException(result->GetError());
		}
	}

	auto table_info = DistributedTableInfo {};
	table_info.table_oid = table_ref.oid;
	table_info.partition_column = data.partition_column;
	table_info.buckets = data.bucket_count;
	MppTables::Get(catalog).AddDistributedTable(transaction.GetDuckContext(), table_info);

	auto all_shards = vector<MppShardInfo> {};
	for (auto &[_, shards] : node_shards) {
		all_shards.insert(all_shards.end(), shards.begin(), shards.end());
	}
	MppShards::Get(catalog).AddShards(transaction.GetDuckContext(), all_shards);
}

} // namespace

TableFunction CreateDistributedTableFunction::GetFunction() {
	auto func = TableFunction();
	func.name = "create_distributed_table";
	func.arguments = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::USMALLINT};
	func.bind = BindCreateDistributedTable;
	func.function = CreateDistributedTableFunc;
	return func;
}

} // namespace duckdb