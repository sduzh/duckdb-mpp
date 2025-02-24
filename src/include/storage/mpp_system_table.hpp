#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {
class BoundConstraint;
class ClientContext;
class DataChunk;
class DuckTransaction;
class MppCatalog;
class StorageIndex;
class TableCatalogEntry;
class TableFilterSet;
class TableScanState;
class TableUpdateState;

class MppSystemTable {
public:
	explicit MppSystemTable(MppCatalog &catalog) : catalog_(catalog) {
	}

	virtual ~MppSystemTable();

	MppCatalog &GetCatalog() {
		return catalog_;
	}

	vector<LogicalType> GetTypes() const;

	void Insert(ClientContext &context, DataChunk &data);

	const ColumnDefinition &GetColumn(const string &name);

protected:
	void Initialize(ClientContext &context, const string &table_name, ColumnList columns,
	                vector<unique_ptr<Constraint>> constraints);

	void InitializeScan(DuckTransaction &transaction, TableScanState &state, const vector<StorageIndex> &column_ids,
	                    TableFilterSet *table_filters = nullptr);

	unique_ptr<TableUpdateState> InitializeUpdate(ClientContext &context,
	                                              const vector<unique_ptr<BoundConstraint>> &bound_constraints);

	void Scan(DuckTransaction &transaction, DataChunk &result, TableScanState &state);

	void Update(TableUpdateState &state, ClientContext &context, Vector &row_ids,
	            const vector<PhysicalIndex> &column_ids, DataChunk &data);

	void ModifyAttachedDatabase(ClientContext &context);

	MppCatalog &catalog_;
	optional_ptr<TableCatalogEntry> table_;
};

} // namespace duckdb