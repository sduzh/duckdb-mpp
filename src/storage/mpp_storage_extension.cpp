#include "storage/mpp_storage_extension.hpp"

#include "common/constants.hpp"
#include "storage/mpp_catalog.hpp"
#include "storage/mpp_transaction_manager.hpp"

namespace duckdb {
unique_ptr<Catalog> MppStorageExtension::AttachMppCatalog(StorageExtensionInfo *storage_info, ClientContext &context,
                                                          AttachedDatabase &db, const string &name, AttachInfo &info,
                                                          AccessMode access_mode) {
	if (info.path.empty()) {
		throw InvalidInputException("attach path is empty!");
	}
	auto &base_catalog = Catalog::GetCatalog(context, info.path);
	if (base_catalog.IsSystemCatalog()) {
		throw InvalidInputException("Cannot use system catalog as the base catalog of MPP");
	}
	if (!base_catalog.IsDuckCatalog()) {
		throw InvalidInputException("Base catalog of MPP must be a duck catalog");
	}
	auto endpoint = Endpoint {};
	for (auto &[key, value] : info.options) {
		if (!StringUtil::CIEquals("endpoint", key)) {
			continue;
		}
		if (endpoint.IsValid()) {
			throw BinderException("Duplicated endpoint in attach options");
		}
		endpoint = Endpoint::Parse(value.ToString());
	}
	if (!endpoint.IsValid()) {
		endpoint = Endpoint("127.0.0.1", MPP_SERVER_DEFAULT_PORT);
	}
	return make_uniq<MppCatalog>(db, base_catalog, endpoint);
}

unique_ptr<TransactionManager> MppStorageExtension::CreateMppTransactionManager(StorageExtensionInfo *storage_info,
                                                                                AttachedDatabase &db,
                                                                                Catalog &catalog) {
	return make_uniq<MppTransactionManager>(db);
}
} // namespace duckdb