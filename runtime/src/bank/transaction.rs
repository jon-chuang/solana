use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::atomic::Ordering::Relaxed,
    time::Instant,
};

use super::{
    debug, inc_new_counter_info, info, is_simple_vote_transaction, log_enabled, warn, Bank,
    ExecuteTimings,
};
use crate::{
    accounts::TransactionLoadResult,
    accounts_db::ErrorCounters,
    hashed_transaction::{HashedTransaction, HashedTransactionSlice},
    instruction_recorder::InstructionRecorder,
    log_collector::LogCollector,
    stakes::Stakes,
    status_cache::StatusCache,
    transaction_batch::TransactionBatch,
    vote_account::ArcVoteAccount,
};
use solana_measure::measure::Measure;
use solana_sdk::{
    account::AccountSharedData,
    clock::{MAX_PROCESSING_AGE, MAX_TRANSACTION_FORWARDING_DELAY},
    fee_calculator::{FeeCalculator, FeeConfig},
    instruction::CompiledInstruction,
    message::Message,
    nonce_account,
    process_instruction::BpfComputeBudget,
    pubkey::Pubkey,
    sanitize::Sanitize,
    signature::{Keypair, Signature},
    system_transaction,
    transaction::{self, Result, Transaction, TransactionError},
};

pub type TransactionCheckResult = (Result<()>, Option<NonceRollbackPartial>);
pub type TransactionExecutionResult = (Result<()>, Option<NonceRollbackFull>);
pub struct TransactionResults {
    pub fee_collection_results: Vec<Result<()>>,
    pub execution_results: Vec<TransactionExecutionResult>,
    pub overwritten_vote_accounts: Vec<OverwrittenVoteAccount>,
}
pub struct TransactionBalancesSet {
    pub pre_balances: TransactionBalances,
    pub post_balances: TransactionBalances,
}
impl TransactionBalancesSet {
    pub fn new(pre_balances: TransactionBalances, post_balances: TransactionBalances) -> Self {
        assert_eq!(pre_balances.len(), post_balances.len());
        Self {
            pre_balances,
            post_balances,
        }
    }
}
pub type TransactionBalances = Vec<Vec<u64>>;

/// An ordered list of instructions that were invoked during a transaction instruction
pub type InnerInstructions = Vec<CompiledInstruction>;

/// A list of instructions that were invoked during each instruction of a transaction
pub type InnerInstructionsList = Vec<InnerInstructions>;

/// A list of log messages emitted during a transaction
pub type TransactionLogMessages = Vec<String>;

#[derive(Serialize, Deserialize, AbiExample, AbiEnumVisitor, Debug, PartialEq)]
pub enum TransactionLogCollectorFilter {
    All,
    AllWithVotes,
    None,
    OnlyMentionedAddresses,
}

impl Default for TransactionLogCollectorFilter {
    fn default() -> Self {
        Self::None
    }
}

#[derive(AbiExample, Debug, Default)]
pub struct TransactionLogCollectorConfig {
    pub mentioned_addresses: HashSet<Pubkey>,
    pub filter: TransactionLogCollectorFilter,
}

#[derive(AbiExample, Clone, Debug)]
pub struct TransactionLogInfo {
    pub signature: Signature,
    pub result: Result<()>,
    pub is_vote: bool,
    pub log_messages: TransactionLogMessages,
}

#[derive(AbiExample, Default, Debug)]
pub struct TransactionLogCollector {
    // All the logs collected for from this Bank.  Exact contents depend on the
    // active `TransactionLogCollectorFilter`
    pub logs: Vec<TransactionLogInfo>,

    // For each `mentioned_addresses`, maintain a list of indices into `logs` to easily
    // locate the logs from transactions that included the mentioned addresses.
    pub mentioned_address_map: HashMap<Pubkey, Vec<usize>>,
}

impl Bank {
    /// Create, sign, and process a Transaction from `keypair` to `to` of
    /// `n` lamports where `blockhash` is the last Entry ID observed by the client.
    pub fn transfer(&self, n: u64, keypair: &Keypair, to: &Pubkey) -> Result<Signature> {
        let blockhash = self.last_blockhash();
        let tx = system_transaction::transfer(keypair, to, n, blockhash);
        let signature = tx.signatures[0];
        self.process_transaction(&tx).map(|_| signature)
    }
    /// Process a batch of transactions.
    #[must_use]
    pub fn load_execute_and_commit_transactions(
        &self,
        batch: &TransactionBatch,
        max_age: usize,
        collect_balances: bool,
        enable_cpi_recording: bool,
        enable_log_recording: bool,
        timings: &mut ExecuteTimings,
    ) -> (
        TransactionResults,
        TransactionBalancesSet,
        Vec<Option<InnerInstructionsList>>,
        Vec<TransactionLogMessages>,
    ) {
        let pre_balances = if collect_balances {
            self.collect_balances(batch)
        } else {
            vec![]
        };

        let (
            mut loaded_accounts,
            executed,
            inner_instructions,
            transaction_logs,
            _,
            tx_count,
            signature_count,
        ) = self.load_and_execute_transactions(
            batch,
            max_age,
            enable_cpi_recording,
            enable_log_recording,
            timings,
        );

        let results = self.commit_transactions(
            batch.hashed_transactions(),
            &mut loaded_accounts,
            &executed,
            tx_count,
            signature_count,
            timings,
        );
        let post_balances = if collect_balances {
            self.collect_balances(batch)
        } else {
            vec![]
        };
        (
            results,
            TransactionBalancesSet::new(pre_balances, post_balances),
            inner_instructions,
            transaction_logs,
        )
    }

    /// Process a Transaction. This is used for unit tests and simply calls the vector
    /// Bank::process_transactions method
    pub fn process_transaction(&self, tx: &Transaction) -> Result<()> {
        let batch = self.prepare_batch(std::iter::once(tx));
        self.process_transaction_batch(&batch)[0].clone()?;
        tx.signatures
            .get(0)
            .map_or(Ok(()), |sig| self.get_signature_status(sig).unwrap())
    }

    #[must_use]
    pub fn process_transactions(&self, txs: &[Transaction]) -> Vec<Result<()>> {
        let batch = self.prepare_batch(txs.iter());
        self.process_transaction_batch(&batch)
    }

    #[must_use]
    fn process_transaction_batch(&self, batch: &TransactionBatch) -> Vec<Result<()>> {
        self.load_execute_and_commit_transactions(
            batch,
            MAX_PROCESSING_AGE,
            false,
            false,
            false,
            &mut ExecuteTimings::default(),
        )
        .0
        .fee_collection_results
    }

    #[allow(clippy::type_complexity)]
    pub fn load_and_execute_transactions(
        &self,
        batch: &TransactionBatch,
        max_age: usize,
        enable_cpi_recording: bool,
        enable_log_recording: bool,
        timings: &mut ExecuteTimings,
    ) -> (
        Vec<TransactionLoadResult>,
        Vec<TransactionExecutionResult>,
        Vec<Option<InnerInstructionsList>>,
        Vec<TransactionLogMessages>,
        Vec<usize>,
        u64,
        u64,
    ) {
        let hashed_txs = batch.hashed_transactions();
        debug!("processing transactions: {}", hashed_txs.len());
        inc_new_counter_info!("bank-process_transactions", hashed_txs.len());
        let mut error_counters = ErrorCounters::default();

        let retryable_txs: Vec<_> = batch
            .lock_results()
            .iter()
            .enumerate()
            .filter_map(|(index, res)| match res {
                Err(TransactionError::AccountInUse) => {
                    error_counters.account_in_use += 1;
                    Some(index)
                }
                Err(_) => None,
                Ok(_) => None,
            })
            .collect();

        let mut check_time = Measure::start("check_transactions");
        let check_results = self.check_transactions(
            hashed_txs,
            batch.lock_results(),
            max_age,
            &mut error_counters,
        );
        check_time.stop();

        let mut load_time = Measure::start("accounts_load");
        let mut loaded_accounts = self.rc.accounts.load_accounts(
            &self.ancestors,
            hashed_txs.as_transactions_iter(),
            check_results,
            &self.blockhash_queue.read().unwrap(),
            &mut error_counters,
            &self.rent_collector,
            &self.feature_set,
        );
        load_time.stop();

        let mut execution_time = Measure::start("execution_time");
        let mut signature_count: u64 = 0;
        let mut inner_instructions: Vec<Option<InnerInstructionsList>> =
            Vec::with_capacity(hashed_txs.len());
        let mut transaction_log_messages = Vec::with_capacity(hashed_txs.len());
        let bpf_compute_budget = self
            .bpf_compute_budget
            .unwrap_or_else(BpfComputeBudget::new);

        let executed: Vec<TransactionExecutionResult> = loaded_accounts
            .iter_mut()
            .zip(hashed_txs.as_transactions_iter())
            .map(|(accs, tx)| match accs {
                (Err(e), _nonce_rollback) => (Err(e.clone()), None),
                (Ok(loaded_transaction), nonce_rollback) => {
                    signature_count += u64::from(tx.message().header.num_required_signatures);
                    let executors = self.get_executors(&tx.message, &loaded_transaction.loaders);

                    let (account_refcells, account_dep_refcells, loader_refcells) =
                        Self::accounts_to_refcells(
                            &mut loaded_transaction.accounts,
                            &mut loaded_transaction.account_deps,
                            &mut loaded_transaction.loaders,
                        );

                    let instruction_recorders = if enable_cpi_recording {
                        let ix_count = tx.message.instructions.len();
                        let mut recorders = Vec::with_capacity(ix_count);
                        recorders.resize_with(ix_count, InstructionRecorder::default);
                        Some(recorders)
                    } else {
                        None
                    };

                    let log_collector = if enable_log_recording {
                        Some(Rc::new(LogCollector::default()))
                    } else {
                        None
                    };

                    let mut process_result = self.message_processor.process_message(
                        tx.message(),
                        &loader_refcells,
                        &account_refcells,
                        &account_dep_refcells,
                        &self.rent_collector,
                        log_collector.clone(),
                        executors.clone(),
                        instruction_recorders.as_deref(),
                        self.feature_set.clone(),
                        bpf_compute_budget,
                        &mut timings.details,
                        self.rc.accounts.clone(),
                        &self.ancestors,
                    );

                    if enable_log_recording {
                        let log_messages: TransactionLogMessages =
                            Rc::try_unwrap(log_collector.unwrap_or_default())
                                .unwrap_or_default()
                                .into();

                        transaction_log_messages.push(log_messages);
                    }

                    Self::compile_recorded_instructions(
                        &mut inner_instructions,
                        instruction_recorders,
                        &tx.message,
                    );

                    if let Err(e) = Self::refcells_to_accounts(
                        &mut loaded_transaction.accounts,
                        &mut loaded_transaction.loaders,
                        account_refcells,
                        loader_refcells,
                    ) {
                        warn!("Account lifetime mismanagement");
                        process_result = Err(e);
                    }

                    if process_result.is_ok() {
                        self.update_executors(executors);
                    }

                    let nonce_rollback =
                        if let Err(TransactionError::InstructionError(_, _)) = &process_result {
                            error_counters.instruction_error += 1;
                            nonce_rollback.clone()
                        } else if process_result.is_err() {
                            None
                        } else {
                            nonce_rollback.clone()
                        };
                    (process_result, nonce_rollback)
                }
            })
            .collect();

        execution_time.stop();

        debug!(
            "check: {}us load: {}us execute: {}us txs_len={}",
            check_time.as_us(),
            load_time.as_us(),
            execution_time.as_us(),
            hashed_txs.len(),
        );
        timings.check_us += check_time.as_us();
        timings.load_us += load_time.as_us();
        timings.execute_us += execution_time.as_us();

        let mut tx_count: u64 = 0;
        let err_count = &mut error_counters.total;
        let transaction_log_collector_config =
            self.transaction_log_collector_config.read().unwrap();

        for (i, ((r, _nonce_rollback), hashed_tx)) in executed.iter().zip(hashed_txs).enumerate() {
            let tx = hashed_tx.transaction();
            if let Some(debug_keys) = &self.transaction_debug_keys {
                for key in &tx.message.account_keys {
                    if debug_keys.contains(key) {
                        info!("slot: {} result: {:?} tx: {:?}", self.slot, r, tx);
                        break;
                    }
                }
            }

            if Self::can_commit(r) // Skip log collection for unprocessed transactions
                && transaction_log_collector_config.filter != TransactionLogCollectorFilter::None
            {
                let mut transaction_log_collector = self.transaction_log_collector.write().unwrap();
                let transaction_log_index = transaction_log_collector.logs.len();

                let mut mentioned_address = false;
                if !transaction_log_collector_config
                    .mentioned_addresses
                    .is_empty()
                {
                    for key in &tx.message.account_keys {
                        if transaction_log_collector_config
                            .mentioned_addresses
                            .contains(key)
                        {
                            transaction_log_collector
                                .mentioned_address_map
                                .entry(*key)
                                .or_default()
                                .push(transaction_log_index);
                            mentioned_address = true;
                        }
                    }
                }

                let is_vote = is_simple_vote_transaction(tx);

                let store = match transaction_log_collector_config.filter {
                    TransactionLogCollectorFilter::All => !is_vote || mentioned_address,
                    TransactionLogCollectorFilter::AllWithVotes => true,
                    TransactionLogCollectorFilter::None => false,
                    TransactionLogCollectorFilter::OnlyMentionedAddresses => mentioned_address,
                };

                if store {
                    transaction_log_collector.logs.push(TransactionLogInfo {
                        signature: tx.signatures[0],
                        result: r.clone(),
                        is_vote,
                        log_messages: transaction_log_messages.get(i).cloned().unwrap_or_default(),
                    });
                }
            }

            if r.is_ok() {
                tx_count += 1;
            } else {
                if *err_count == 0 {
                    debug!("tx error: {:?} {:?}", r, tx);
                }
                *err_count += 1;
            }
        }
        if *err_count > 0 {
            debug!(
                "{} errors of {} txs",
                *err_count,
                *err_count as u64 + tx_count
            );
        }
        Self::update_error_counters(&error_counters);
        (
            loaded_accounts,
            executed,
            inner_instructions,
            transaction_log_messages,
            retryable_txs,
            tx_count,
            signature_count,
        )
    }

    pub(super) fn filter_program_errors_and_collect_fee<'a>(
        &self,
        txs: impl Iterator<Item = &'a Transaction>,
        executed: &[TransactionExecutionResult],
    ) -> Vec<Result<()>> {
        let hash_queue = self.blockhash_queue.read().unwrap();
        let mut fees = 0;

        let fee_config = FeeConfig {
            secp256k1_program_enabled: self.secp256k1_program_enabled(),
        };

        let results = txs
            .zip(executed)
            .map(|(tx, (res, nonce_rollback))| {
                let (fee_calculator, is_durable_nonce) = nonce_rollback
                    .as_ref()
                    .map(|nonce_rollback| nonce_rollback.fee_calculator())
                    .map(|maybe_fee_calculator| (maybe_fee_calculator, true))
                    .unwrap_or_else(|| {
                        (
                            hash_queue
                                .get_fee_calculator(&tx.message().recent_blockhash)
                                .cloned(),
                            false,
                        )
                    });
                let fee_calculator = fee_calculator.ok_or(TransactionError::BlockhashNotFound)?;

                let fee = fee_calculator.calculate_fee_with_config(tx.message(), &fee_config);

                let message = tx.message();
                match *res {
                    Err(TransactionError::InstructionError(_, _)) => {
                        // credit the transaction fee even in case of InstructionError
                        // necessary to withdraw from account[0] here because previous
                        // work of doing so (in accounts.load()) is ignored by store_account()
                        //
                        // ...except nonce accounts, which will have their post-load,
                        // pre-execute account state stored
                        if !is_durable_nonce {
                            self.withdraw(&message.account_keys[0], fee)?;
                        }
                        fees += fee;
                        Ok(())
                    }
                    Ok(()) => {
                        fees += fee;
                        Ok(())
                    }
                    _ => res.clone(),
                }
            })
            .collect();

        self.collector_fees.fetch_add(fees, Relaxed);
        results
    }

    pub fn commit_transactions(
        &self,
        hashed_txs: &[HashedTransaction],
        loaded_accounts: &mut [TransactionLoadResult],
        executed: &[TransactionExecutionResult],
        tx_count: u64,
        signature_count: u64,
        timings: &mut ExecuteTimings,
    ) -> TransactionResults {
        assert!(
            !self.freeze_started(),
            "commit_transactions() working on a bank that is already frozen or is undergoing freezing!"
        );

        self.increment_transaction_count(tx_count);
        self.increment_signature_count(signature_count);

        inc_new_counter_info!("bank-process_transactions-txs", tx_count as usize);
        inc_new_counter_info!("bank-process_transactions-sigs", signature_count as usize);

        if !hashed_txs.is_empty() {
            let processed_tx_count = hashed_txs.len() as u64;
            let failed_tx_count = processed_tx_count.saturating_sub(tx_count);
            self.transaction_error_count
                .fetch_add(failed_tx_count, Relaxed);
            self.transaction_entries_count.fetch_add(1, Relaxed);
            self.transactions_per_entry_max
                .fetch_max(processed_tx_count, Relaxed);
        }

        if executed
            .iter()
            .any(|(res, _nonce_rollback)| Self::can_commit(res))
        {
            self.is_delta.store(true, Relaxed);
        }

        let mut write_time = Measure::start("write_time");
        self.rc.accounts.store_cached(
            self.slot(),
            hashed_txs.as_transactions_iter(),
            executed,
            loaded_accounts,
            &self.rent_collector,
            &self.last_blockhash_with_fee_calculator(),
            self.fix_recent_blockhashes_sysvar_delay(),
            self.demote_sysvar_write_locks(),
        );
        self.collect_rent(executed, loaded_accounts);

        let overwritten_vote_accounts = self.update_cached_accounts(
            hashed_txs.as_transactions_iter(),
            executed,
            loaded_accounts,
        );

        // once committed there is no way to unroll
        write_time.stop();
        debug!(
            "store: {}us txs_len={}",
            write_time.as_us(),
            hashed_txs.len()
        );
        timings.store_us += write_time.as_us();
        self.update_transaction_statuses(hashed_txs, &executed);
        let fee_collection_results =
            self.filter_program_errors_and_collect_fee(hashed_txs.as_transactions_iter(), executed);

        TransactionResults {
            fee_collection_results,
            execution_results: executed.to_vec(),
            overwritten_vote_accounts,
        }
    }

    pub fn prepare_batch<'a, 'b>(
        &'a self,
        txs: impl Iterator<Item = &'b Transaction>,
    ) -> TransactionBatch<'a, 'b> {
        let hashed_txs: Vec<HashedTransaction> = txs.map(HashedTransaction::from).collect();
        let lock_results = self.rc.accounts.lock_accounts(
            hashed_txs.as_transactions_iter(),
            self.demote_sysvar_write_locks(),
        );
        TransactionBatch::new(lock_results, &self, Cow::Owned(hashed_txs))
    }

    pub fn prepare_hashed_batch<'a, 'b>(
        &'a self,
        hashed_txs: &'b [HashedTransaction],
    ) -> TransactionBatch<'a, 'b> {
        let lock_results = self.rc.accounts.lock_accounts(
            hashed_txs.as_transactions_iter(),
            self.demote_sysvar_write_locks(),
        );
        TransactionBatch::new(lock_results, &self, Cow::Borrowed(hashed_txs))
    }

    pub fn prepare_simulation_batch<'a, 'b>(
        &'a self,
        txs: &'b [Transaction],
    ) -> TransactionBatch<'a, 'b> {
        let lock_results: Vec<_> = txs
            .iter()
            .map(|tx| tx.sanitize().map_err(|e| e.into()))
            .collect();
        let hashed_txs = txs.iter().map(HashedTransaction::from).collect();
        let mut batch = TransactionBatch::new(lock_results, &self, hashed_txs);
        batch.needs_unlock = false;
        batch
    }

    /// Run transactions against a frozen bank without committing the results
    pub fn simulate_transaction(
        &self,
        transaction: Transaction,
    ) -> (Result<()>, TransactionLogMessages) {
        assert!(self.is_frozen(), "simulation bank must be frozen");

        let txs = &[transaction];
        let batch = self.prepare_simulation_batch(txs);

        let mut timings = ExecuteTimings::default();

        let (
            _loaded_accounts,
            executed,
            _inner_instructions,
            log_messages,
            _retryable_transactions,
            _transaction_count,
            _signature_count,
        ) = self.load_and_execute_transactions(
            &batch,
            // After simulation, transactions will need to be forwarded to the leader
            // for processing. During forwarding, the transaction could expire if the
            // delay is not accounted for.
            MAX_PROCESSING_AGE - MAX_TRANSACTION_FORWARDING_DELAY,
            false,
            true,
            &mut timings,
        );

        let transaction_result = executed[0].0.clone().map(|_| ());
        let log_messages = log_messages
            .get(0)
            .map_or(vec![], |messages| messages.to_vec());

        debug!("simulate_transaction: {:?}", timings);

        (transaction_result, log_messages)
    }

    pub(super) fn check_age<'a>(
        &self,
        txs: impl Iterator<Item = &'a Transaction>,
        lock_results: Vec<Result<()>>,
        max_age: usize,
        error_counters: &mut ErrorCounters,
    ) -> Vec<TransactionCheckResult> {
        let hash_queue = self.blockhash_queue.read().unwrap();
        txs.zip(lock_results)
            .map(|(tx, lock_res)| match lock_res {
                Ok(()) => {
                    let message = tx.message();
                    let hash_age = hash_queue.check_hash_age(&message.recent_blockhash, max_age);
                    if hash_age == Some(true) {
                        (Ok(()), None)
                    } else if let Some((pubkey, acc)) = self.check_tx_durable_nonce(&tx) {
                        (Ok(()), Some(NonceRollbackPartial::new(pubkey, acc)))
                    } else if hash_age == Some(false) {
                        error_counters.blockhash_too_old += 1;
                        (Err(TransactionError::BlockhashNotFound), None)
                    } else {
                        error_counters.blockhash_not_found += 1;
                        (Err(TransactionError::BlockhashNotFound), None)
                    }
                }
                Err(e) => (Err(e), None),
            })
            .collect()
    }

    fn is_tx_already_processed(
        &self,
        hashed_tx: &HashedTransaction,
        status_cache: &StatusCache<Result<()>>,
        check_duplicates_by_hash_enabled: bool,
    ) -> bool {
        let tx = hashed_tx.transaction();
        let status_cache_key: Option<&[u8]> = if check_duplicates_by_hash_enabled {
            Some(hashed_tx.message_hash.as_ref())
        } else {
            tx.signatures.get(0).map(|sig0| sig0.as_ref())
        };
        status_cache_key
            .and_then(|key| {
                status_cache.get_status(key, &tx.message().recent_blockhash, &self.ancestors)
            })
            .is_some()
    }

    pub(super) fn check_status_cache(
        &self,
        hashed_txs: &[HashedTransaction],
        lock_results: Vec<TransactionCheckResult>,
        error_counters: &mut ErrorCounters,
    ) -> Vec<TransactionCheckResult> {
        let rcache = self.src.status_cache.read().unwrap();
        let check_duplicates_by_hash_enabled = self.check_duplicates_by_hash_enabled();
        hashed_txs
            .iter()
            .zip(lock_results)
            .map(|(hashed_tx, (lock_res, nonce_rollback))| {
                if lock_res.is_ok()
                    && self.is_tx_already_processed(
                        hashed_tx,
                        &rcache,
                        check_duplicates_by_hash_enabled,
                    )
                {
                    error_counters.already_processed += 1;
                    return (Err(TransactionError::AlreadyProcessed), None);
                }

                (lock_res, nonce_rollback)
            })
            .collect()
    }

    pub(super) fn filter_by_vote_transactions<'a>(
        &self,
        txs: impl Iterator<Item = &'a Transaction>,
        lock_results: Vec<TransactionCheckResult>,
        error_counters: &mut ErrorCounters,
    ) -> Vec<TransactionCheckResult> {
        txs.zip(lock_results)
            .map(|(tx, lock_res)| {
                if lock_res.0.is_ok() {
                    if is_simple_vote_transaction(tx) {
                        return lock_res;
                    }

                    error_counters.not_allowed_during_cluster_maintenance += 1;
                    return (Err(TransactionError::ClusterMaintenance), lock_res.1);
                }
                lock_res
            })
            .collect()
    }
    pub fn check_tx_durable_nonce(&self, tx: &Transaction) -> Option<(Pubkey, AccountSharedData)> {
        transaction::uses_durable_nonce(&tx)
            .and_then(|nonce_ix| transaction::get_nonce_pubkey_from_instruction(&nonce_ix, &tx))
            .and_then(|nonce_pubkey| {
                self.get_account(&nonce_pubkey)
                    .map(|acc| (*nonce_pubkey, acc))
            })
            .filter(|(_pubkey, nonce_account)| {
                nonce_account::verify_nonce_account(nonce_account, &tx.message().recent_blockhash)
            })
    }

    pub fn check_transactions(
        &self,
        hashed_txs: &[HashedTransaction],
        lock_results: &[Result<()>],
        max_age: usize,
        mut error_counters: &mut ErrorCounters,
    ) -> Vec<TransactionCheckResult> {
        let age_results = self.check_age(
            hashed_txs.as_transactions_iter(),
            lock_results.to_vec(),
            max_age,
            &mut error_counters,
        );
        let cache_results = self.check_status_cache(hashed_txs, age_results, &mut error_counters);
        if self.upgrade_epoch() {
            // Reject all non-vote transactions
            self.filter_by_vote_transactions(
                hashed_txs.as_transactions_iter(),
                cache_results,
                &mut error_counters,
            )
        } else {
            cache_results
        }
    }

    fn update_transaction_statuses(
        &self,
        hashed_txs: &[HashedTransaction],
        res: &[TransactionExecutionResult],
    ) {
        let mut status_cache = self.src.status_cache.write().unwrap();
        assert_eq!(hashed_txs.len(), res.len());
        for (hashed_tx, (res, _nonce_rollback)) in hashed_txs.iter().zip(res) {
            let tx = hashed_tx.transaction();
            if Self::can_commit(res) && !tx.signatures.is_empty() {
                status_cache.insert(
                    &tx.message().recent_blockhash,
                    &tx.signatures[0],
                    self.slot(),
                    res.clone(),
                );
                status_cache.insert(
                    &tx.message().recent_blockhash,
                    &hashed_tx.message_hash,
                    self.slot(),
                    res.clone(),
                );
            }
        }
    }

    /// a bank-level cache of vote accounts
    fn update_cached_accounts<'a>(
        &self,
        txs: impl Iterator<Item = &'a Transaction>,
        res: &[TransactionExecutionResult],
        loaded: &[TransactionLoadResult],
    ) -> Vec<OverwrittenVoteAccount> {
        let mut overwritten_vote_accounts = vec![];
        for (i, ((raccs, _load_nonce_rollback), tx)) in loaded.iter().zip(txs).enumerate() {
            let (res, _res_nonce_rollback) = &res[i];
            if res.is_err() || raccs.is_err() {
                continue;
            }

            let message = &tx.message();
            let loaded_transaction = raccs.as_ref().unwrap();

            for (pubkey, account) in message
                .account_keys
                .iter()
                .zip(loaded_transaction.accounts.iter())
                .filter(|(_key, account)| (Stakes::is_stake(account)))
            {
                if Stakes::is_stake(account) {
                    if let Some(old_vote_account) = self.stakes.write().unwrap().store(
                        pubkey,
                        account,
                        self.stake_program_v2_enabled(),
                        self.check_init_vote_data_enabled(),
                    ) {
                        // TODO: one of the indices is redundant.
                        overwritten_vote_accounts.push(OverwrittenVoteAccount {
                            account: old_vote_account,
                            transaction_index: i,
                            transaction_result_index: i,
                        });
                    }
                }
            }
        }

        overwritten_vote_accounts
    }

    pub fn get_transaction_logs(
        &self,
        address: Option<&Pubkey>,
    ) -> Option<Vec<TransactionLogInfo>> {
        let transaction_log_collector = self.transaction_log_collector.read().unwrap();

        match address {
            None => Some(transaction_log_collector.logs.clone()),
            Some(address) => transaction_log_collector
                .mentioned_address_map
                .get(address)
                .map(|log_indices| {
                    log_indices
                        .iter()
                        .map(|i| transaction_log_collector.logs[*i].clone())
                        .collect()
                }),
        }
    }

    pub fn transaction_count(&self) -> u64 {
        self.transaction_count.load(Relaxed)
    }

    pub fn transaction_error_count(&self) -> u64 {
        self.transaction_error_count.load(Relaxed)
    }

    pub fn transaction_entries_count(&self) -> u64 {
        self.transaction_entries_count.load(Relaxed)
    }

    pub fn transactions_per_entry_max(&self) -> u64 {
        self.transactions_per_entry_max.load(Relaxed)
    }

    fn increment_transaction_count(&self, tx_count: u64) {
        self.transaction_count.fetch_add(tx_count, Relaxed);
    }

    // Check if the wallclock time from bank creation to now has exceeded the allotted
    // time for transaction processing
    pub fn should_bank_still_be_processing_txs(
        bank_creation_time: &Instant,
        max_tx_ingestion_nanos: u128,
    ) -> bool {
        // Do this check outside of the poh lock, hence not a method on PohRecorder
        bank_creation_time.elapsed().as_nanos() <= max_tx_ingestion_nanos
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        bank::{goto_end_of_slot, test_utils::new_from_parent},
        genesis_utils::{
            bootstrap_validator_stake_lamports, create_genesis_config_with_leader,
            GenesisConfigInfo,
        },
    };
    use solana_sdk::{
        account_utils::StateMut,
        clock::MAX_RECENT_BLOCKHASHES,
        fee_calculator::FeeRateGovernor,
        genesis_config::{create_genesis_config, GenesisConfig},
        hash::Hash,
        instruction::InstructionError,
        nonce,
        signature::{keypair_from_seed, Keypair, Signer},
        system_instruction::{self, SystemError},
        system_program, system_transaction, sysvar,
    };
    use std::sync::Arc;

    #[test]
    fn test_two_payments_to_one_party() {
        let (genesis_config, mint_keypair) = create_genesis_config(10_000);
        let pubkey = solana_sdk::pubkey::new_rand();
        let bank = Bank::new(&genesis_config);
        assert_eq!(bank.last_blockhash(), genesis_config.hash());

        bank.transfer(1_000, &mint_keypair, &pubkey).unwrap();
        assert_eq!(bank.get_balance(&pubkey), 1_000);

        bank.transfer(500, &mint_keypair, &pubkey).unwrap();
        assert_eq!(bank.get_balance(&pubkey), 1_500);
        assert_eq!(bank.transaction_count(), 2);
    }

    #[test]
    fn test_one_source_two_tx_one_batch() {
        let (genesis_config, mint_keypair) = create_genesis_config(1);
        let key1 = solana_sdk::pubkey::new_rand();
        let key2 = solana_sdk::pubkey::new_rand();
        let bank = Bank::new(&genesis_config);
        assert_eq!(bank.last_blockhash(), genesis_config.hash());

        let t1 = system_transaction::transfer(&mint_keypair, &key1, 1, genesis_config.hash());
        let t2 = system_transaction::transfer(&mint_keypair, &key2, 1, genesis_config.hash());
        let res = bank.process_transactions(&[t1.clone(), t2.clone()]);

        assert_eq!(res.len(), 2);
        assert_eq!(res[0], Ok(()));
        assert_eq!(res[1], Err(TransactionError::AccountInUse));
        assert_eq!(bank.get_balance(&mint_keypair.pubkey()), 0);
        assert_eq!(bank.get_balance(&key1), 1);
        assert_eq!(bank.get_balance(&key2), 0);
        assert_eq!(bank.get_signature_status(&t1.signatures[0]), Some(Ok(())));
        // TODO: Transactions that fail to pay a fee could be dropped silently.
        // Non-instruction errors don't get logged in the signature cache
        assert_eq!(bank.get_signature_status(&t2.signatures[0]), None);
    }

    #[test]
    fn test_one_tx_two_out_atomic_fail() {
        let (genesis_config, mint_keypair) = create_genesis_config(1);
        let key1 = solana_sdk::pubkey::new_rand();
        let key2 = solana_sdk::pubkey::new_rand();
        let bank = Bank::new(&genesis_config);
        let instructions =
            system_instruction::transfer_many(&mint_keypair.pubkey(), &[(key1, 1), (key2, 1)]);
        let message = Message::new(&instructions, Some(&mint_keypair.pubkey()));
        let tx = Transaction::new(&[&mint_keypair], message, genesis_config.hash());
        assert_eq!(
            bank.process_transaction(&tx).unwrap_err(),
            TransactionError::InstructionError(1, SystemError::ResultWithNegativeLamports.into())
        );
        assert_eq!(bank.get_balance(&mint_keypair.pubkey()), 1);
        assert_eq!(bank.get_balance(&key1), 0);
        assert_eq!(bank.get_balance(&key2), 0);
    }

    #[test]
    fn test_one_tx_two_out_atomic_pass() {
        let (genesis_config, mint_keypair) = create_genesis_config(2);
        let key1 = solana_sdk::pubkey::new_rand();
        let key2 = solana_sdk::pubkey::new_rand();
        let bank = Bank::new(&genesis_config);
        let instructions =
            system_instruction::transfer_many(&mint_keypair.pubkey(), &[(key1, 1), (key2, 1)]);
        let message = Message::new(&instructions, Some(&mint_keypair.pubkey()));
        let tx = Transaction::new(&[&mint_keypair], message, genesis_config.hash());
        bank.process_transaction(&tx).unwrap();
        assert_eq!(bank.get_balance(&mint_keypair.pubkey()), 0);
        assert_eq!(bank.get_balance(&key1), 1);
        assert_eq!(bank.get_balance(&key2), 1);
    }

    // This test demonstrates that fees are paid even when a program fails.
    #[test]
    fn test_detect_failed_duplicate_transactions() {
        let (mut genesis_config, mint_keypair) = create_genesis_config(2);
        genesis_config.fee_rate_governor = FeeRateGovernor::new(1, 0);
        let bank = Bank::new(&genesis_config);

        let dest = Keypair::new();

        // source with 0 program context
        let tx =
            system_transaction::transfer(&mint_keypair, &dest.pubkey(), 2, genesis_config.hash());
        let signature = tx.signatures[0];
        assert!(!bank.has_signature(&signature));

        assert_eq!(
            bank.process_transaction(&tx),
            Err(TransactionError::InstructionError(
                0,
                SystemError::ResultWithNegativeLamports.into(),
            ))
        );

        // The lamports didn't move, but the from address paid the transaction fee.
        assert_eq!(bank.get_balance(&dest.pubkey()), 0);

        // This should be the original balance minus the transaction fee.
        assert_eq!(bank.get_balance(&mint_keypair.pubkey()), 1);
    }

    #[test]
    fn test_account_not_found() {
        solana_logger::setup();
        let (genesis_config, mint_keypair) = create_genesis_config(0);
        let bank = Bank::new(&genesis_config);
        let keypair = Keypair::new();
        assert_eq!(
            bank.transfer(1, &keypair, &mint_keypair.pubkey()),
            Err(TransactionError::AccountNotFound)
        );
        assert_eq!(bank.transaction_count(), 0);
    }

    #[test]
    fn test_insufficient_funds() {
        let (genesis_config, mint_keypair) = create_genesis_config(11_000);
        let bank = Bank::new(&genesis_config);
        let pubkey = solana_sdk::pubkey::new_rand();
        bank.transfer(1_000, &mint_keypair, &pubkey).unwrap();
        assert_eq!(bank.transaction_count(), 1);
        assert_eq!(bank.get_balance(&pubkey), 1_000);
        assert_eq!(
            bank.transfer(10_001, &mint_keypair, &pubkey),
            Err(TransactionError::InstructionError(
                0,
                SystemError::ResultWithNegativeLamports.into(),
            ))
        );
        assert_eq!(bank.transaction_count(), 1);

        let mint_pubkey = mint_keypair.pubkey();
        assert_eq!(bank.get_balance(&mint_pubkey), 10_000);
        assert_eq!(bank.get_balance(&pubkey), 1_000);
    }

    #[test]
    fn test_transfer_to_newb() {
        solana_logger::setup();
        let (genesis_config, mint_keypair) = create_genesis_config(10_000);
        let bank = Bank::new(&genesis_config);
        let pubkey = solana_sdk::pubkey::new_rand();
        bank.transfer(500, &mint_keypair, &pubkey).unwrap();
        assert_eq!(bank.get_balance(&pubkey), 500);
    }

    #[test]
    fn test_transfer_to_sysvar() {
        solana_logger::setup();
        let (genesis_config, mint_keypair) = create_genesis_config(10_000);
        let bank = Arc::new(Bank::new(&genesis_config));

        let normal_pubkey = solana_sdk::pubkey::new_rand();
        let sysvar_pubkey = sysvar::clock::id();
        assert_eq!(bank.get_balance(&normal_pubkey), 0);
        assert_eq!(bank.get_balance(&sysvar_pubkey), 1);

        bank.transfer(500, &mint_keypair, &normal_pubkey).unwrap();
        bank.transfer(500, &mint_keypair, &sysvar_pubkey).unwrap();
        assert_eq!(bank.get_balance(&normal_pubkey), 500);
        assert_eq!(bank.get_balance(&sysvar_pubkey), 501);

        let bank = Arc::new(new_from_parent(&bank));
        assert_eq!(bank.get_balance(&normal_pubkey), 500);
        assert_eq!(bank.get_balance(&sysvar_pubkey), 501);
    }

    #[test]
    fn test_nonce_rollback_info() {
        let nonce_authority = keypair_from_seed(&[0; 32]).unwrap();
        let nonce_address = nonce_authority.pubkey();
        let fee_calculator = FeeCalculator::new(42);
        let state =
            nonce::state::Versions::new_current(nonce::State::Initialized(nonce::state::Data {
                authority: Pubkey::default(),
                blockhash: Hash::new_unique(),
                fee_calculator: fee_calculator.clone(),
            }));
        let nonce_account = AccountSharedData::new_data(43, &state, &system_program::id()).unwrap();

        // NonceRollbackPartial create + NonceRollbackInfo impl
        let partial = NonceRollbackPartial::new(nonce_address, nonce_account.clone());
        assert_eq!(*partial.nonce_address(), nonce_address);
        assert_eq!(*partial.nonce_account(), nonce_account);
        assert_eq!(partial.fee_calculator(), Some(fee_calculator.clone()));
        assert_eq!(partial.fee_account(), None);

        let from = keypair_from_seed(&[1; 32]).unwrap();
        let from_address = from.pubkey();
        let to_address = Pubkey::new_unique();
        let instructions = vec![
            system_instruction::advance_nonce_account(&nonce_address, &nonce_authority.pubkey()),
            system_instruction::transfer(&from_address, &to_address, 42),
        ];
        let message = Message::new(&instructions, Some(&from_address));

        let from_account = AccountSharedData::new(44, 0, &Pubkey::default());
        let to_account = AccountSharedData::new(45, 0, &Pubkey::default());
        let recent_blockhashes_sysvar_account = AccountSharedData::new(4, 0, &Pubkey::default());
        let accounts = [
            from_account.clone(),
            nonce_account.clone(),
            to_account.clone(),
            recent_blockhashes_sysvar_account.clone(),
        ];

        // NonceRollbackFull create + NonceRollbackInfo impl
        let full = NonceRollbackFull::from_partial(partial.clone(), &message, &accounts).unwrap();
        assert_eq!(*full.nonce_address(), nonce_address);
        assert_eq!(*full.nonce_account(), nonce_account);
        assert_eq!(full.fee_calculator(), Some(fee_calculator));
        assert_eq!(full.fee_account(), Some(&from_account));

        let message = Message::new(&instructions, Some(&nonce_address));
        let accounts = [
            nonce_account,
            from_account,
            to_account,
            recent_blockhashes_sysvar_account,
        ];

        // Nonce account is fee-payer
        let full = NonceRollbackFull::from_partial(partial.clone(), &message, &accounts).unwrap();
        assert_eq!(full.fee_account(), None);

        // NonceRollbackFull create, fee-payer not in account_keys fails
        assert_eq!(
            NonceRollbackFull::from_partial(partial, &message, &[]).unwrap_err(),
            TransactionError::AccountNotFound,
        );
    }
    #[test]
    fn test_filter_program_errors_and_collect_fee() {
        let leader = solana_sdk::pubkey::new_rand();
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_leader(100, &leader, 3);
        genesis_config.fee_rate_governor = FeeRateGovernor::new(2, 0);
        let bank = Bank::new(&genesis_config);

        let key = Keypair::new();
        let tx1 =
            system_transaction::transfer(&mint_keypair, &key.pubkey(), 2, genesis_config.hash());
        let tx2 =
            system_transaction::transfer(&mint_keypair, &key.pubkey(), 5, genesis_config.hash());

        let results = vec![
            (Ok(()), None),
            (
                Err(TransactionError::InstructionError(
                    1,
                    SystemError::ResultWithNegativeLamports.into(),
                )),
                None,
            ),
        ];
        let initial_balance = bank.get_balance(&leader);

        let results = bank.filter_program_errors_and_collect_fee([tx1, tx2].iter(), &results);
        bank.freeze();
        assert_eq!(
            bank.get_balance(&leader),
            initial_balance
                + bank
                    .fee_rate_governor
                    .burn(bank.fee_calculator.lamports_per_signature * 2)
                    .0
        );
        assert_eq!(results[0], Ok(()));
        assert_eq!(results[1], Ok(()));
    }

    #[test]
    fn test_pre_post_transaction_balances() {
        let (mut genesis_config, _mint_keypair) = create_genesis_config(500);
        let fee_rate_governor = FeeRateGovernor::new(1, 0);
        genesis_config.fee_rate_governor = fee_rate_governor;
        let parent = Arc::new(Bank::new(&genesis_config));
        let bank0 = Arc::new(new_from_parent(&parent));

        let keypair0 = Keypair::new();
        let keypair1 = Keypair::new();
        let pubkey0 = solana_sdk::pubkey::new_rand();
        let pubkey1 = solana_sdk::pubkey::new_rand();
        let pubkey2 = solana_sdk::pubkey::new_rand();
        let keypair0_account = AccountSharedData::new(8, 0, &Pubkey::default());
        let keypair1_account = AccountSharedData::new(9, 0, &Pubkey::default());
        let account0 = AccountSharedData::new(11, 0, &&Pubkey::default());
        bank0.store_account(&keypair0.pubkey(), &keypair0_account);
        bank0.store_account(&keypair1.pubkey(), &keypair1_account);
        bank0.store_account(&pubkey0, &account0);

        let blockhash = bank0.last_blockhash();

        let tx0 = system_transaction::transfer(&keypair0, &pubkey0, 2, blockhash);
        let tx1 = system_transaction::transfer(&Keypair::new(), &pubkey1, 2, blockhash);
        let tx2 = system_transaction::transfer(&keypair1, &pubkey2, 12, blockhash);
        let txs = vec![tx0, tx1, tx2];

        let lock_result = bank0.prepare_batch(txs.iter());
        let (transaction_results, transaction_balances_set, inner_instructions, transaction_logs) =
            bank0.load_execute_and_commit_transactions(
                &lock_result,
                MAX_PROCESSING_AGE,
                true,
                false,
                false,
                &mut ExecuteTimings::default(),
            );

        assert!(inner_instructions[0].iter().all(|ix| ix.is_empty()));
        assert_eq!(transaction_logs.len(), 0);

        assert_eq!(transaction_balances_set.pre_balances.len(), 3);
        assert_eq!(transaction_balances_set.post_balances.len(), 3);

        assert!(transaction_results.execution_results[0].0.is_ok());
        assert_eq!(transaction_balances_set.pre_balances[0], vec![8, 11, 1]);
        assert_eq!(transaction_balances_set.post_balances[0], vec![5, 13, 1]);

        // Failed transactions still produce balance sets
        // This is a TransactionError - not possible to charge fees
        assert!(transaction_results.execution_results[1].0.is_err());
        assert_eq!(transaction_balances_set.pre_balances[1], vec![0, 0, 1]);
        assert_eq!(transaction_balances_set.post_balances[1], vec![0, 0, 1]);

        // Failed transactions still produce balance sets
        // This is an InstructionError - fees charged
        assert!(transaction_results.execution_results[2].0.is_err());
        assert_eq!(transaction_balances_set.pre_balances[2], vec![9, 0, 1]);
        assert_eq!(transaction_balances_set.post_balances[2], vec![8, 0, 1]);
    }

    #[test]
    fn test_transaction_with_duplicate_accounts_in_instruction() {
        let (genesis_config, mint_keypair) = create_genesis_config(500);
        let mut bank = Bank::new(&genesis_config);

        fn mock_process_instruction(
            _program_id: &Pubkey,
            data: &[u8],
            invoke_context: &mut dyn InvokeContext,
        ) -> result::Result<(), InstructionError> {
            let keyed_accounts = invoke_context.get_keyed_accounts()?;
            let lamports = data[0] as u64;
            {
                let mut to_account = keyed_accounts[1].try_account_ref_mut()?;
                let mut dup_account = keyed_accounts[2].try_account_ref_mut()?;
                dup_account.checked_sub_lamports(lamports)?;
                to_account.checked_add_lamports(lamports)?;
            }
            keyed_accounts[0]
                .try_account_ref_mut()?
                .checked_sub_lamports(lamports)?;
            keyed_accounts[1]
                .try_account_ref_mut()?
                .checked_add_lamports(lamports)?;
            Ok(())
        }

        let mock_program_id = Pubkey::new(&[2u8; 32]);
        bank.add_builtin("mock_program", mock_program_id, mock_process_instruction);

        let from_pubkey = solana_sdk::pubkey::new_rand();
        let to_pubkey = solana_sdk::pubkey::new_rand();
        let dup_pubkey = from_pubkey;
        let from_account = AccountSharedData::new(100, 1, &mock_program_id);
        let to_account = AccountSharedData::new(0, 1, &mock_program_id);
        bank.store_account(&from_pubkey, &from_account);
        bank.store_account(&to_pubkey, &to_account);

        let account_metas = vec![
            AccountMeta::new(from_pubkey, false),
            AccountMeta::new(to_pubkey, false),
            AccountMeta::new(dup_pubkey, false),
        ];
        let instruction = Instruction::new_with_bincode(mock_program_id, &10, account_metas);
        let tx = Transaction::new_signed_with_payer(
            &[instruction],
            Some(&mint_keypair.pubkey()),
            &[&mint_keypair],
            bank.last_blockhash(),
        );

        let result = bank.process_transaction(&tx);
        assert_eq!(result, Ok(()));
        assert_eq!(bank.get_balance(&from_pubkey), 80);
        assert_eq!(bank.get_balance(&to_pubkey), 20);
    }

    #[test]
    fn test_transaction_with_program_ids_passed_to_programs() {
        let (genesis_config, mint_keypair) = create_genesis_config(500);
        let mut bank = Bank::new(&genesis_config);

        #[allow(clippy::unnecessary_wraps)]
        fn mock_process_instruction(
            _program_id: &Pubkey,
            _data: &[u8],
            _invoke_context: &mut dyn InvokeContext,
        ) -> result::Result<(), InstructionError> {
            Ok(())
        }

        let mock_program_id = Pubkey::new(&[2u8; 32]);
        bank.add_builtin("mock_program", mock_program_id, mock_process_instruction);

        let from_pubkey = solana_sdk::pubkey::new_rand();
        let to_pubkey = solana_sdk::pubkey::new_rand();
        let dup_pubkey = from_pubkey;
        let from_account = AccountSharedData::new(100, 1, &mock_program_id);
        let to_account = AccountSharedData::new(0, 1, &mock_program_id);
        bank.store_account(&from_pubkey, &from_account);
        bank.store_account(&to_pubkey, &to_account);

        let account_metas = vec![
            AccountMeta::new(from_pubkey, false),
            AccountMeta::new(to_pubkey, false),
            AccountMeta::new(dup_pubkey, false),
            AccountMeta::new(mock_program_id, false),
        ];
        let instruction = Instruction::new_with_bincode(mock_program_id, &10, account_metas);
        let tx = Transaction::new_signed_with_payer(
            &[instruction],
            Some(&mint_keypair.pubkey()),
            &[&mint_keypair],
            bank.last_blockhash(),
        );

        let result = bank.process_transaction(&tx);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_duplicate_account_key() {
        solana_logger::setup();
        let (genesis_config, mint_keypair) = create_genesis_config(500);
        let mut bank = Bank::new(&genesis_config);

        let from_pubkey = solana_sdk::pubkey::new_rand();
        let to_pubkey = solana_sdk::pubkey::new_rand();

        let account_metas = vec![
            AccountMeta::new(from_pubkey, false),
            AccountMeta::new(to_pubkey, false),
        ];

        bank.add_builtin(
            "mock_vote",
            solana_vote_program::id(),
            mock_ok_vote_processor,
        );

        let instruction =
            Instruction::new_with_bincode(solana_vote_program::id(), &10, account_metas);
        let mut tx = Transaction::new_signed_with_payer(
            &[instruction],
            Some(&mint_keypair.pubkey()),
            &[&mint_keypair],
            bank.last_blockhash(),
        );
        tx.message.account_keys.push(from_pubkey);

        let result = bank.process_transaction(&tx);
        assert_eq!(result, Err(TransactionError::AccountLoadedTwice));
    }

    #[test]
    fn test_account_ids_after_program_ids() {
        solana_logger::setup();
        let (genesis_config, mint_keypair) = create_genesis_config(500);
        let mut bank = Bank::new(&genesis_config);

        let from_pubkey = solana_sdk::pubkey::new_rand();
        let to_pubkey = solana_sdk::pubkey::new_rand();

        let account_metas = vec![
            AccountMeta::new(from_pubkey, false),
            AccountMeta::new(to_pubkey, false),
        ];

        let instruction =
            Instruction::new_with_bincode(solana_vote_program::id(), &10, account_metas);
        let mut tx = Transaction::new_signed_with_payer(
            &[instruction],
            Some(&mint_keypair.pubkey()),
            &[&mint_keypair],
            bank.last_blockhash(),
        );

        tx.message.account_keys.push(solana_sdk::pubkey::new_rand());

        bank.add_builtin(
            "mock_vote",
            solana_vote_program::id(),
            mock_ok_vote_processor,
        );
        let result = bank.process_transaction(&tx);
        assert_eq!(result, Ok(()));
        let account = bank.get_account(&solana_vote_program::id()).unwrap();
        info!("account: {:?}", account);
        assert!(account.executable());
    }

    #[test]
    fn test_tx_log_order() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_leader(
            1_000_000_000_000_000,
            &Pubkey::new_unique(),
            bootstrap_validator_stake_lamports(),
        );
        let bank = Arc::new(Bank::new(&genesis_config));
        *bank.transaction_log_collector_config.write().unwrap() = TransactionLogCollectorConfig {
            mentioned_addresses: HashSet::new(),
            filter: TransactionLogCollectorFilter::All,
        };
        let blockhash = bank.last_blockhash();

        let sender0 = Keypair::new();
        let sender1 = Keypair::new();
        bank.transfer(100, &mint_keypair, &sender0.pubkey())
            .unwrap();
        bank.transfer(100, &mint_keypair, &sender1.pubkey())
            .unwrap();

        let recipient0 = Pubkey::new_unique();
        let recipient1 = Pubkey::new_unique();
        let tx0 = system_transaction::transfer(&sender0, &recipient0, 10, blockhash);
        let success_sig = tx0.signatures[0];
        let tx1 = system_transaction::transfer(&sender1, &recipient1, 110, blockhash); // Should produce insufficient funds log
        let failure_sig = tx1.signatures[0];
        let txs = vec![tx1, tx0];
        let batch = bank.prepare_batch(txs.iter());

        let log_results = bank
            .load_execute_and_commit_transactions(
                &batch,
                MAX_PROCESSING_AGE,
                false,
                false,
                true,
                &mut ExecuteTimings::default(),
            )
            .3;
        assert_eq!(log_results.len(), 2);
        assert!(log_results[0]
            .clone()
            .pop()
            .unwrap()
            .contains(&"failed".to_string()));
        assert!(log_results[1]
            .clone()
            .pop()
            .unwrap()
            .contains(&"success".to_string()));

        let stored_logs = &bank.transaction_log_collector.read().unwrap().logs;
        let success_log_info = stored_logs
            .iter()
            .find(|transaction_log_info| transaction_log_info.signature == success_sig)
            .unwrap();
        assert!(success_log_info.result.is_ok());
        let success_log = success_log_info.log_messages.clone().pop().unwrap();
        assert!(success_log.contains(&"success".to_string()));
        let failure_log_info = stored_logs
            .iter()
            .find(|transaction_log_info| transaction_log_info.signature == failure_sig)
            .unwrap();
        assert!(failure_log_info.result.is_err());
        let failure_log = failure_log_info.log_messages.clone().pop().unwrap();
        assert!(failure_log.contains(&"failed".to_string()));
    }
}
