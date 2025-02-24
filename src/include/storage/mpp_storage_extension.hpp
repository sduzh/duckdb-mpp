#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {
class MppStorageExtension final : public StorageExtension {
public:
	MppStorageExtension() {
		attach = AttachMppCatalog;
		create_transaction_manager = CreateMppTransactionManager;
	}

private:
	static unique_ptr<Catalog> AttachMppCatalog(StorageExtensionInfo *storage_info, ClientContext &context,
	                                            AttachedDatabase &db, const string &name, AttachInfo &info,
	                                            AccessMode access_mode);

	static unique_ptr<TransactionManager> CreateMppTransactionManager(StorageExtensionInfo *storage_info,
	                                                                  AttachedDatabase &db, Catalog &catalog);
};

} // namespace duckdb