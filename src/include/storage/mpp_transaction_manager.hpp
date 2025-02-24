#pragma once

#include "duckdb.hpp"

namespace duckdb {

class MppCatalog;
class MppTransaction;

class MppTransactionManager final : public TransactionManager {
public:
	explicit MppTransactionManager(AttachedDatabase &db);

	~MppTransactionManager() override;

	Transaction &StartTransaction(ClientContext &context) override;

	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;

	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

	bool IsDuckTransactionManager() override {
		return false;
	}

private:
	MppCatalog &catalog_;

	mutex lock_;
	transaction_t current_transaction_id_;
	//! Set of currently running transactions
	map<transaction_t, unique_ptr<MppTransaction>> active_transactions_;
};

} // namespace duckdb