#include "storage/mpp_nodes.hpp"

#include "common/constants.hpp"
#include "common/endpoint.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "storage/mpp_catalog.hpp"

namespace duckdb {

static const auto kTableName = "nodes";
static const auto kHostColumn = ColumnDefinition("host", LogicalType::VARCHAR);
static const auto kPortColumn = ColumnDefinition("port", LogicalType::USMALLINT);

void MppNodes::Initialize(ClientContext &context) {
	auto columns = ColumnList {};
	columns.AddColumn(kHostColumn.Copy());
	columns.AddColumn(kPortColumn.Copy());

	auto constraints = vector<unique_ptr<Constraint>> {};
	constraints.emplace_back(
	    make_uniq<UniqueConstraint>(vector<string> {kHostColumn.GetName(), kPortColumn.GetName()}, true));
	constraints.emplace_back(make_uniq<NotNullConstraint>(LogicalIndex(0)));
	constraints.emplace_back(make_uniq<NotNullConstraint>(LogicalIndex(1)));

	MppSystemTable::Initialize(context, kTableName, std::move(columns), std::move(constraints));

	try {
		AddNode(context, catalog_.GetLocalEndpoint());
	} catch (const ConstraintException &e) {
		// ignore
	}
}

MppNodes &MppNodes::Get(MppCatalog &mpp_catalog) {
	return *mpp_catalog.GetNodeManager();
}

auto MppNodes::GetNodes(ClientContext &context) -> vector<Endpoint> {
	auto nodes = vector<Endpoint>();
	auto &table = *table_;
	auto storage_ids = vector<StorageIndex>();
	for (auto &col : table.GetColumns().Logical()) {
		storage_ids.emplace_back(col.StorageOid());
	}
	auto scan_state = TableScanState();
	scan_state.Initialize(storage_ids);
	auto &storage = table.GetStorage();
	auto &duck_tx = DuckTransaction::Get(context, catalog_.GetBase());
	auto data_chunk = DataChunk();
	storage.InitializeScan(duck_tx, scan_state, storage_ids);
	data_chunk.Initialize(context, {LogicalType::VARCHAR, LogicalType::USMALLINT});
	do {
		data_chunk.Reset();
		storage.Scan(duck_tx, data_chunk, scan_state);
		for (idx_t row = 0, num_row = data_chunk.size(); row < num_row; ++row) {
			auto host = StringValue::Get(data_chunk.GetValue(0, row));
			auto port = USmallIntValue::Get(data_chunk.GetValue(1, row));
			nodes.emplace_back(host, port);
		}
	} while (data_chunk.size());
	return nodes;
}

void MppNodes::AddNode(ClientContext &context, const Endpoint &node) {
	auto insert_chunk = DataChunk {};
	insert_chunk.Initialize(context, GetTypes(), 1);
	insert_chunk.SetCardinality(1);
	insert_chunk.data[0].SetValue(0, StringVector::AddString(insert_chunk.data[0], node.ip));
	insert_chunk.data[1].SetValue(0, node.port);

	MppSystemTable::Insert(context, insert_chunk);
}

void MppNodes::RemoveNode(ClientContext &context, const Endpoint &node) {
	throw NotImplementedException("NodeManager::RemoveNode");
}
} // namespace duckdb