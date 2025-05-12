#include "storage/mpp_table_entry.hpp"

#include "common/distributed_table_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "function/mpp_table_scan.hpp"
#include "storage/mpp_catalog.hpp"
#include "storage/mpp_schema_entry.hpp"
#include "storage/mpp_tables.hpp"

namespace duckdb {

MppTableEntry::~MppTableEntry() = default;

unique_ptr<MppTableEntry> MppTableEntry::WrapDuckTable(MppCatalog &catalog, MppSchemaEntry &schema,
                                                       TableCatalogEntry &base) {
	D_ASSERT(&catalog.GetBase() == &base.ParentCatalog());
	auto base_info = unique_ptr_cast<CreateInfo, CreateTableInfo>(base.GetInfo());
	base_info->catalog = catalog.GetName();
	base_info->schema = schema.name;
	auto ret = make_uniq<MppTableEntry>(catalog, schema, *base_info, base);
	ret->oid = base.oid;
	return ret;
}

unique_ptr<BaseStatistics> MppTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	throw NotImplementedException("MppTableEntry::GetStatistics");
}

TableFunction MppTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	bind_data = make_uniq<MppTableScanBindData>(context, *this);
	return MppTableScanFunction::GetFunction();
}

TableStorageInfo MppTableEntry::GetStorageInfo(ClientContext &context) {
	return {};
}

void MppTableEntry::GetDistributedTableInfo(ClientContext &context, DistributedTableInfo &info) {
	MppTables::Get(context).GetDistributedTableInfo(context, base_.oid, info);
}

} // namespace duckdb