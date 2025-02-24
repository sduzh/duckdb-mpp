#include "storage/mpp_transaction_manager.hpp"

#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "common/distributed_table_info.hpp"
#include "storage/mpp_catalog.hpp"
#include "storage/mpp_tables.hpp"
#include "storage/mpp_transaction.hpp"

namespace duckdb {
MppTransactionManager::MppTransactionManager(AttachedDatabase &db)
    : TransactionManager(db), catalog_(db.GetCatalog().Cast<MppCatalog>()), lock_(), current_transaction_id_ {2},
      active_transactions_() {
}

MppTransactionManager::~MppTransactionManager() = default;

Transaction &MppTransactionManager::StartTransaction(ClientContext &context) {
	auto duck_conn = make_uniq<Connection>(catalog_.GetBase().GetDatabase());
	duck_conn->SetAutoCommit(false);
	auto transaction = make_uniq<MppTransaction>(*this, context, std::move(duck_conn));
	auto &txn_ref = *transaction;
	auto guard = lock_guard(lock_);
	transaction->transaction_id = current_transaction_id_++;
	active_transactions_.emplace(transaction->transaction_id, std::move(transaction));
	return txn_ref;
}

ErrorData MppTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &mpp_transaction = transaction.Cast<MppTransaction>();
	auto &duck_context = *mpp_transaction.duck_conn->context;
	duck_context.transaction.Commit();

	auto guard = lock_guard(lock_);
	active_transactions_.erase(mpp_transaction.transaction_id);
	return {};
}

void MppTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &mpp_transaction = transaction.Cast<MppTransaction>();
	auto transaction_id = mpp_transaction.transaction_id;
	mpp_transaction.duck_conn->context->transaction.Rollback(nullptr);

	auto guard = lock_guard(lock_);
	active_transactions_.erase(transaction_id);
}

void MppTransactionManager::Checkpoint(ClientContext &context, bool force) {
}

} // namespace duckdb