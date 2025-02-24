#include "duckdb/catalog/catalog.hpp"

#include "common/endpoint.hpp"

namespace duckdb {

class MppSchemaEntry;
class MppServer;
class MppNodes;
class MppShards;
class MppTables;
class ShuffleManager;

class MppCatalog final : public Catalog {
public:
	explicit MppCatalog(AttachedDatabase &database, Catalog &base, Endpoint local_endpoint);

	~MppCatalog() override;

	void Initialize(bool load_builtin) override;

	string GetCatalogType() override {
		return "mpp";
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	using Catalog::GetSchema;

	optional_ptr<SchemaCatalogEntry> GetSchema(CatalogTransaction transaction, const string &schema_name,
	                                           OnEntryNotFound if_not_found, QueryErrorContext error_context) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	unique_ptr<PhysicalOperator> PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
	                                               unique_ptr<PhysicalOperator> plan) override;

	unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op,
	                                        unique_ptr<PhysicalOperator> plan) override;

	unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context, LogicalDelete &op,
	                                        unique_ptr<PhysicalOperator> plan) override;

	unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context, LogicalUpdate &op,
	                                        unique_ptr<PhysicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	vector<MetadataBlockInfo> GetMetadataInfo(ClientContext &context) override;

	bool InMemory() override;

	string GetDBPath() override;

	Catalog &GetBase() const {
		return base_;
	}

	optional_ptr<MppNodes> GetNodeManager() {
		return node_manager_;
	}

	optional_ptr<MppShards> GetShardManager() {
		return shard_manager_;
	}

	optional_ptr<MppTables> GetTableManager() {
		return table_manager_;
	}

	optional_ptr<ShuffleManager> GetShuffleManager() {
		return shuffle_manager_;
	}

	const Endpoint &GetLocalEndpoint() const {
		return local_endpoint_;
	}

	static string InternalSchemaName(const string &schema);

	static bool IsInternalSchema(const string &name);

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;
	void InitDefaultSchema(ClientContext &context);

	Catalog &base_;
	Endpoint local_endpoint_;

	unique_ptr<MppNodes> node_manager_;
	unique_ptr<MppShards> shard_manager_;
	unique_ptr<MppTables> table_manager_;
	unique_ptr<MppServer> mpp_server_;
	unique_ptr<ShuffleManager> shuffle_manager_;
};

} // namespace duckdb