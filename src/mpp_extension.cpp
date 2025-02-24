#define DUCKDB_EXTENSION_MAIN

#include "mpp_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension_util.hpp"

#include "function/master_add_node.hpp"
#include "function/remote_query.hpp"
#include "function/shuffle_read.hpp"
#include "parser/mpp_parser_extension.hpp"
#include "storage/mpp_storage_extension.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	ExtensionUtil::RegisterFunction(instance, MasterAddNodeFunction::GetFunction());
	ExtensionUtil::RegisterFunction(instance, RemoteQueryFunction::GetFunction());
	ExtensionUtil::RegisterFunction(instance, ShuffleReadFunction::GetFunction());

	instance.config.storage_extensions["mpp"] = make_uniq<MppStorageExtension>();
	instance.config.parser_extensions.emplace_back(MppParserExtension());
}

void MppExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string MppExtension::Name() {
	return "mpp";
}

std::string MppExtension::Version() const {
#ifdef EXT_VERSION_SWARM
	return EXT_VERSION_SWARM;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void mpp_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::MppExtension>();
}

DUCKDB_EXTENSION_API const char *mpp_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
