//! The `bank` module tracks client accounts and the progress of on-chain
//! programs.
//!
//! A single bank relates to a block produced by a single leader and each bank
//! except for the genesis bank points back to a parent bank.
//!
//! The bank is the main entrypoint for processing verified transactions with the function
//! `Bank::process_transactions`
//!
//! It does this by loading the accounts using the reference it holds on the account store,
//! and then passing those to the message_processor which handles loading the programs specified
//! by the Transaction and executing it.
//!
//! The bank then stores the results to the accounts store.
//!
//! It then has apis for retrieving if a transaction has been processed and it's status.
//! See `get_signature_status` et al.
//!
//! Bank lifecycle:
//!
//! A bank is newly created and open to transactions. Transactions are applied
//! until either the bank reached the tick count when the node is the leader for that slot, or the
//! node has applied all transactions present in all `Entry`s in the slot.
//!
//! Once it is complete, the bank can then be frozen. After frozen, no more transactions can
//! be applied or state changes made. At the frozen step, rent will be applied and various
//! sysvar special accounts update to the new state of the system.
//!
//! After frozen, and the bank has had the appropriate number of votes on it, then it can become
//! rooted. At this point, it will not be able to be removed from the chain and the
//! state is finalized.
//!
//! It offers a high-level API that signs transactions
//! on behalf of the caller, and a low-level API for when they have
//! already been signed and verified.
use crate::{
    accounts::{
        AccountAddressFilter, Accounts, TransactionAccountDeps, TransactionAccounts,
        TransactionLoadResult, TransactionLoaders,
    },
    accounts_db::{ErrorCounters, SnapshotStorages},
    accounts_index::{AccountIndex, Ancestors, IndexKey},
    blockhash_queue::BlockhashQueue,
    builtins::{self, ActivationType},
    epoch_stakes::{EpochStakes, NodeVoteAccounts},
    hashed_transaction::{HashedTransaction, HashedTransactionSlice},
    inline_spl_token_v2_0,
    instruction_recorder::InstructionRecorder,
    message_processor::{ExecuteDetailsTimings, MessageProcessor},
    rent_collector::RentCollector,
    stakes::Stakes,
    status_cache::{SlotDelta, StatusCache},
    system_instruction_processor::{get_system_account_kind, SystemAccountKind},
    transaction_batch::TransactionBatch,
    vote_account::ArcVoteAccount,
};
use byteorder::{ByteOrder, LittleEndian};
use itertools::Itertools;
use log::*;
use rayon::ThreadPool;
use solana_measure::measure::Measure;
use solana_metrics::{datapoint_debug, inc_new_counter_debug, inc_new_counter_info};
use solana_sdk::{
    account::{
        create_account_shared_data_with_fields as create_account, from_account, Account,
        AccountSharedData, InheritableAccountFields, ReadableAccount, WritableAccount,
    },
    clock::{
        Epoch, Slot, SlotCount, SlotIndex, UnixTimestamp, DEFAULT_TICKS_PER_SECOND,
        INITIAL_RENT_EPOCH, MAX_PROCESSING_AGE, MAX_RECENT_BLOCKHASHES, SECONDS_PER_DAY,
    },
    epoch_info::EpochInfo,
    epoch_schedule::EpochSchedule,
    feature,
    feature_set::{self, FeatureSet},
    fee_calculator::{FeeCalculator, FeeRateGovernor},
    genesis_config::{ClusterType, GenesisConfig},
    hard_forks::HardForks,
    hash::{extend_and_hash, hashv, Hash},
    incinerator,
    inflation::Inflation,
    lamports::LamportsError,
    message::Message,
    native_loader,
    native_token::sol_to_lamports,
    nonce, nonce_account,
    process_instruction::{BpfComputeBudget, ProcessInstructionWithContext},
    program_utils::limited_deserialize,
    pubkey::Pubkey,
    recent_blockhashes_account,
    sanitize::Sanitize,
    signature::{Keypair, Signature},
    slot_history::SlotHistory,
    stake_weighted_timestamp::{calculate_stake_weighted_timestamp, MaxAllowableDrift},
    system_transaction,
    sysvar::{self},
    timing::years_as_slots,
    transaction::{self, Result, Transaction, TransactionError},
};
use solana_stake_program::stake_state::{
    self, Delegation, InflationPointCalculationEvent, PointValue,
};
use solana_vote_program::vote_instruction::VoteInstruction;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    convert::{TryFrom, TryInto},
    fmt, mem,
    ops::RangeInclusive,
    path::PathBuf,
    ptr,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        {Arc, RwLock, RwLockReadGuard},
    },
    time::Duration,
};

mod bank_accounts;
mod bank_feature_set;
mod executors;
mod native_and_builtin;
mod rent;
#[cfg(test)]
pub(crate) mod test_utils;
mod time;
mod transactions;
mod update_sysvar;

use executors::{CachedExecutors, CowCachedExecutors, MAX_CACHED_EXECUTORS};
// use transactions::
pub use bank_accounts::*;
pub use native_and_builtin::{Builtin, Builtins};
pub use transactions::{
    InnerInstructionsList, NonceRollbackFull, NonceRollbackInfo, NonceRollbackPartial,
    OverwrittenVoteAccount, TransactionBalances, TransactionBalancesSet, TransactionCheckResult,
    TransactionExecutionResult, TransactionLogCollector, TransactionLogCollectorConfig,
    TransactionLogCollectorFilter, TransactionLogInfo, TransactionLogMessages, TransactionResults,
};

pub const SECONDS_PER_YEAR: f64 = 365.25 * 24.0 * 60.0 * 60.0;

pub const MAX_LEADER_SCHEDULE_STAKES: Epoch = 5;

#[derive(Default, Debug)]
pub struct ExecuteTimings {
    pub check_us: u64,
    pub load_us: u64,
    pub execute_us: u64,
    pub store_us: u64,
    pub details: ExecuteDetailsTimings,
}

impl ExecuteTimings {
    pub fn accumulate(&mut self, other: &ExecuteTimings) {
        self.check_us += other.check_us;
        self.load_us += other.load_us;
        self.execute_us += other.execute_us;
        self.store_us += other.store_us;
        self.details.accumulate(&other.details);
    }
}

// Can't derive PartialEq because RwLock doesn't implement PartialEq
impl PartialEq for Bank {
    fn eq(&self, other: &Self) -> bool {
        if ptr::eq(self, other) {
            return true;
        }
        *self.blockhash_queue.read().unwrap() == *other.blockhash_queue.read().unwrap()
            && self.ancestors == other.ancestors
            && *self.hash.read().unwrap() == *other.hash.read().unwrap()
            && self.parent_hash == other.parent_hash
            && self.parent_slot == other.parent_slot
            && *self.hard_forks.read().unwrap() == *other.hard_forks.read().unwrap()
            && self.transaction_count.load(Relaxed) == other.transaction_count.load(Relaxed)
            && self.tick_height.load(Relaxed) == other.tick_height.load(Relaxed)
            && self.signature_count.load(Relaxed) == other.signature_count.load(Relaxed)
            && self.capitalization.load(Relaxed) == other.capitalization.load(Relaxed)
            && self.max_tick_height == other.max_tick_height
            && self.hashes_per_tick == other.hashes_per_tick
            && self.ticks_per_slot == other.ticks_per_slot
            && self.ns_per_slot == other.ns_per_slot
            && self.genesis_creation_time == other.genesis_creation_time
            && self.slots_per_year == other.slots_per_year
            && self.unused == other.unused
            && self.slot == other.slot
            && self.epoch == other.epoch
            && self.block_height == other.block_height
            && self.collector_id == other.collector_id
            && self.collector_fees.load(Relaxed) == other.collector_fees.load(Relaxed)
            && self.fee_calculator == other.fee_calculator
            && self.fee_rate_governor == other.fee_rate_governor
            && self.collected_rent.load(Relaxed) == other.collected_rent.load(Relaxed)
            && self.rent_collector == other.rent_collector
            && self.epoch_schedule == other.epoch_schedule
            && *self.inflation.read().unwrap() == *other.inflation.read().unwrap()
            && *self.stakes.read().unwrap() == *other.stakes.read().unwrap()
            && self.epoch_stakes == other.epoch_stakes
            && self.is_delta.load(Relaxed) == other.is_delta.load(Relaxed)
    }
}

/// Manager for the state of all accounts and programs after processing its entries.
/// AbiExample is needed even without Serialize/Deserialize; actual (de-)serialization
/// are implemented elsewhere for versioning
#[derive(AbiExample, Debug, Default)]
pub struct Bank {
    /// References to accounts, parent and signature status
    pub rc: BankRc,

    pub src: StatusCacheRc,

    /// FIFO queue of `recent_blockhash` items
    blockhash_queue: RwLock<BlockhashQueue>,

    /// The set of parents including this bank
    pub ancestors: Ancestors,

    /// Hash of this Bank's state. Only meaningful after freezing.
    hash: RwLock<Hash>,

    /// Hash of this Bank's parent's state
    parent_hash: Hash,

    /// parent's slot
    parent_slot: Slot,

    /// slots to hard fork at
    hard_forks: Arc<RwLock<HardForks>>,

    /// The number of transactions processed without error
    transaction_count: AtomicU64,

    /// The number of transaction errors in this slot
    transaction_error_count: AtomicU64,

    /// The number of transaction entries in this slot
    transaction_entries_count: AtomicU64,

    /// The max number of transaction in an entry in this slot
    transactions_per_entry_max: AtomicU64,

    /// Bank tick height
    tick_height: AtomicU64,

    /// The number of signatures from valid transactions in this slot
    signature_count: AtomicU64,

    /// Total capitalization, used to calculate inflation
    capitalization: AtomicU64,

    // Bank max_tick_height
    max_tick_height: u64,

    /// The number of hashes in each tick. None value means hashing is disabled.
    hashes_per_tick: Option<u64>,

    /// The number of ticks in each slot.
    ticks_per_slot: u64,

    /// length of a slot in ns
    pub ns_per_slot: u128,

    /// genesis time, used for computed clock
    genesis_creation_time: UnixTimestamp,

    /// The number of slots per year, used for inflation
    slots_per_year: f64,

    /// Unused
    unused: u64,

    /// Bank slot (i.e. block)
    slot: Slot,

    /// Bank epoch
    epoch: Epoch,

    /// Bank block_height
    block_height: u64,

    /// The pubkey to send transactions fees to.
    collector_id: Pubkey,

    /// Fees that have been collected
    collector_fees: AtomicU64,

    /// Latest transaction fees for transactions processed by this bank
    fee_calculator: FeeCalculator,

    /// Track cluster signature throughput and adjust fee rate
    fee_rate_governor: FeeRateGovernor,

    /// Rent that has been collected
    collected_rent: AtomicU64,

    /// latest rent collector, knows the epoch
    rent_collector: RentCollector,

    /// initialized from genesis
    epoch_schedule: EpochSchedule,

    /// inflation specs
    inflation: Arc<RwLock<Inflation>>,

    /// cache of vote_account and stake_account state for this fork
    stakes: RwLock<Stakes>,

    /// staked nodes on epoch boundaries, saved off when a bank.slot() is at
    ///   a leader schedule calculation boundary
    epoch_stakes: HashMap<Epoch, EpochStakes>,

    /// A boolean reflecting whether any entries were recorded into the PoH
    /// stream for the slot == self.slot
    is_delta: AtomicBool,

    /// The Message processor
    message_processor: MessageProcessor,

    bpf_compute_budget: Option<BpfComputeBudget>,

    /// Builtin programs activated dynamically by feature
    #[allow(clippy::rc_buffer)]
    feature_builtins: Arc<Vec<(Builtin, Pubkey, ActivationType)>>,

    /// Last time when the cluster info vote listener has synced with this bank
    pub last_vote_sync: AtomicU64,

    /// Protocol-level rewards that were distributed by this bank
    pub rewards: RwLock<Vec<(Pubkey, RewardInfo)>>,

    pub skip_drop: AtomicBool,

    pub cluster_type: Option<ClusterType>,

    pub lazy_rent_collection: AtomicBool,

    pub no_stake_rewrite: AtomicBool,

    // this is temporary field only to remove rewards_pool entirely
    pub rewards_pool_pubkeys: Arc<HashSet<Pubkey>>,

    /// Cached executors
    cached_executors: RwLock<CowCachedExecutors>,

    transaction_debug_keys: Option<Arc<HashSet<Pubkey>>>,

    // Global configuration for how transaction logs should be collected across all banks
    pub transaction_log_collector_config: Arc<RwLock<TransactionLogCollectorConfig>>,

    // Logs from transactions that this Bank executed collected according to the criteria in
    // `transaction_log_collector_config`
    pub transaction_log_collector: Arc<RwLock<TransactionLogCollector>>,

    pub feature_set: Arc<FeatureSet>,

    pub drop_callback: RwLock<OptionalDropCallback>,

    pub freeze_started: AtomicBool,
}

impl Default for BlockhashQueue {
    fn default() -> Self {
        Self::new(MAX_RECENT_BLOCKHASHES)
    }
}

impl Bank {
    /// Returns all ancestors excluding self.slot.
    pub(crate) fn proper_ancestors(&self) -> impl Iterator<Item = Slot> + '_ {
        self.ancestors
            .keys()
            .copied()
            .filter(move |slot| *slot != self.slot)
    }

    pub fn set_callback(&self, callback: Option<Box<dyn DropCallback + Send + Sync>>) {
        *self.drop_callback.write().unwrap() = OptionalDropCallback(callback);
    }

    pub fn collector_id(&self) -> &Pubkey {
        &self.collector_id
    }

    pub fn slot(&self) -> Slot {
        self.slot
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn first_normal_epoch(&self) -> Epoch {
        self.epoch_schedule.first_normal_epoch
    }

    pub fn freeze_lock(&self) -> RwLockReadGuard<Hash> {
        self.hash.read().unwrap()
    }

    pub fn hash(&self) -> Hash {
        *self.hash.read().unwrap()
    }

    pub fn is_frozen(&self) -> bool {
        *self.hash.read().unwrap() != Hash::default()
    }

    pub fn freeze_started(&self) -> bool {
        self.freeze_started.load(Relaxed)
    }

    pub fn status_cache_ancestors(&self) -> Vec<u64> {
        let mut roots = self.src.status_cache.read().unwrap().roots().clone();
        let min = roots.iter().min().cloned().unwrap_or(0);
        for ancestor in self.ancestors.keys() {
            if *ancestor >= min {
                roots.insert(*ancestor);
            }
        }

        let mut ancestors: Vec<_> = roots.into_iter().collect();
        #[allow(clippy::stable_sort_primitive)]
        ancestors.sort();
        ancestors
    }

    fn inherit_specially_retained_account_fields(
        &self,
        old_account: &Option<AccountSharedData>,
    ) -> InheritableAccountFields {
        (
            old_account.as_ref().map(|a| a.lamports()).unwrap_or(1),
            INITIAL_RENT_EPOCH,
        )
    }

    /// Unused conversion
    pub fn get_unused_from_slot(rooted_slot: Slot, unused: u64) -> u64 {
        (rooted_slot + (unused - 1)) / unused
    }

    fn rewrite_stakes(&self) -> (usize, usize) {
        let mut examined_count = 0;
        let mut rewritten_count = 0;
        self.cloned_stake_delegations()
            .into_iter()
            .for_each(|(stake_pubkey, _delegation)| {
                examined_count += 1;
                if let Some(mut stake_account) = self.get_account_with_fixed_root(&stake_pubkey) {
                    if let Ok(result) =
                        stake_state::rewrite_stakes(&mut stake_account, &self.rent_collector.rent)
                    {
                        self.store_account(&stake_pubkey, &stake_account);
                        let message = format!("rewrote stake: {}, {:?}", stake_pubkey, result);
                        info!("{}", message);
                        datapoint_info!("stake_info", ("info", message, String));
                        rewritten_count += 1;
                    }
                }
            });

        info!(
            "bank (slot: {}): rewrite_stakes: {} accounts rewritten / {} accounts examined",
            self.slot(),
            rewritten_count,
            examined_count,
        );
        datapoint_info!(
            "rewrite-stakes",
            ("examined_count", examined_count, i64),
            ("rewritten_count", rewritten_count, i64)
        );

        (examined_count, rewritten_count)
    }

    // Calculates the starting-slot for inflation from the activation slot.
    // This method assumes that `pico_inflation` will be enabled before `full_inflation`, giving
    // precedence to the latter. However, since `pico_inflation` is fixed-rate Inflation, should
    // `pico_inflation` be enabled 2nd, the incorrect start slot provided here should have no
    // effect on the inflation calculation.
    fn get_inflation_start_slot(&self) -> Slot {
        let mut slots = self
            .feature_set
            .full_inflation_features_enabled()
            .iter()
            .filter_map(|id| self.feature_set.activated_slot(&id))
            .collect::<Vec<_>>();
        slots.sort_unstable();
        slots.get(0).cloned().unwrap_or_else(|| {
            self.feature_set
                .activated_slot(&feature_set::pico_inflation::id())
                .unwrap_or(0)
        })
    }

    fn get_inflation_num_slots(&self) -> u64 {
        let inflation_activation_slot = self.get_inflation_start_slot();
        // Normalize inflation_start to align with the start of rewards accrual.
        let inflation_start_slot = self.epoch_schedule.get_first_slot_in_epoch(
            self.epoch_schedule
                .get_epoch(inflation_activation_slot)
                .saturating_sub(1),
        );
        self.epoch_schedule.get_first_slot_in_epoch(self.epoch()) - inflation_start_slot
    }

    pub fn slot_in_year_for_inflation(&self) -> f64 {
        let num_slots = self.get_inflation_num_slots();

        // calculated as: num_slots / (slots / year)
        num_slots as f64 / self.slots_per_year
    }

    // update rewards based on the previous epoch
    fn update_rewards(
        &mut self,
        prev_epoch: Epoch,
        reward_calc_tracer: &mut Option<impl FnMut(&RewardCalculationEvent)>,
    ) {
        if prev_epoch == self.epoch() {
            return;
        }
        // if I'm the first Bank in an epoch, count, claim, disburse rewards from Inflation

        let slot_in_year = self.slot_in_year_for_inflation();
        let epoch_duration_in_years = self.epoch_duration_in_years(prev_epoch);

        let (validator_rate, foundation_rate) = {
            let inflation = self.inflation.read().unwrap();
            (
                (*inflation).validator(slot_in_year),
                (*inflation).foundation(slot_in_year),
            )
        };

        let capitalization = self.capitalization();
        let validator_rewards =
            (validator_rate * capitalization as f64 * epoch_duration_in_years) as u64;

        let old_vote_balance_and_staked = self.stakes.read().unwrap().vote_balance_and_staked();

        let validator_point_value = self.pay_validator_rewards(
            prev_epoch,
            validator_rewards,
            reward_calc_tracer,
            self.stake_program_v2_enabled(),
        );

        if !self
            .feature_set
            .is_active(&feature_set::deprecate_rewards_sysvar::id())
        {
            // this sysvar can be retired once `pico_inflation` is enabled on all clusters
            self.update_sysvar_account(&sysvar::rewards::id(), |account| {
                create_account(
                    &sysvar::rewards::Rewards::new(validator_point_value),
                    self.inherit_specially_retained_account_fields(account),
                )
            });
        }

        let new_vote_balance_and_staked = self.stakes.read().unwrap().vote_balance_and_staked();
        let validator_rewards_paid = new_vote_balance_and_staked - old_vote_balance_and_staked;
        assert_eq!(
            validator_rewards_paid,
            u64::try_from(
                self.rewards
                    .read()
                    .unwrap()
                    .iter()
                    .map(|(_address, reward_info)| {
                        match reward_info.reward_type {
                            RewardType::Voting | RewardType::Staking => reward_info.lamports,
                            _ => 0,
                        }
                    })
                    .sum::<i64>()
            )
            .unwrap()
        );

        // verify that we didn't pay any more than we expected to
        assert!(validator_rewards >= validator_rewards_paid);

        info!(
            "distributed inflation: {} (rounded from: {})",
            validator_rewards_paid, validator_rewards
        );

        self.capitalization
            .fetch_add(validator_rewards_paid, Relaxed);

        let active_stake = if let Some(stake_history_entry) =
            self.stakes.read().unwrap().history().get(&prev_epoch)
        {
            stake_history_entry.effective
        } else {
            0
        };

        datapoint_warn!(
            "epoch_rewards",
            ("slot", self.slot, i64),
            ("epoch", prev_epoch, i64),
            ("validator_rate", validator_rate, f64),
            ("foundation_rate", foundation_rate, f64),
            ("epoch_duration_in_years", epoch_duration_in_years, f64),
            ("validator_rewards", validator_rewards_paid, i64),
            ("active_stake", active_stake, i64),
            ("pre_capitalization", capitalization, i64),
            ("post_capitalization", self.capitalization(), i64)
        );
    }

    /// map stake delegations into resolved (pubkey, account) pairs
    ///  returns a map (has to be copied) of loaded
    ///   ( Vec<(staker info)> (voter account) ) keyed by voter pubkey
    ///
    /// Filters out invalid pairs
    fn stake_delegation_accounts(
        &self,
        reward_calc_tracer: &mut Option<impl FnMut(&RewardCalculationEvent)>,
    ) -> HashMap<Pubkey, (Vec<(Pubkey, AccountSharedData)>, AccountSharedData)> {
        let mut accounts = HashMap::new();

        self.stakes
            .read()
            .unwrap()
            .stake_delegations()
            .iter()
            .for_each(|(stake_pubkey, delegation)| {
                match (
                    self.get_account_with_fixed_root(&stake_pubkey),
                    self.get_account_with_fixed_root(&delegation.voter_pubkey),
                ) {
                    (Some(stake_account), Some(vote_account)) => {
                        // call tracer to catch any illegal data if any
                        if let Some(reward_calc_tracer) = reward_calc_tracer {
                            reward_calc_tracer(&RewardCalculationEvent::Staking(
                                stake_pubkey,
                                &InflationPointCalculationEvent::Delegation(
                                    *delegation,
                                    *vote_account.owner(),
                                ),
                            ));
                        }
                        if self
                            .feature_set
                            .is_active(&feature_set::filter_stake_delegation_accounts::id())
                            && (stake_account.owner() != &solana_stake_program::id()
                                || vote_account.owner() != &solana_vote_program::id())
                        {
                            datapoint_warn!(
                                "bank-stake_delegation_accounts-invalid-account",
                                ("slot", self.slot() as i64, i64),
                                ("stake-address", format!("{:?}", stake_pubkey), String),
                                (
                                    "vote-address",
                                    format!("{:?}", delegation.voter_pubkey),
                                    String
                                ),
                            );
                            return;
                        }
                        let entry = accounts
                            .entry(delegation.voter_pubkey)
                            .or_insert((Vec::new(), vote_account));
                        entry.0.push((*stake_pubkey, stake_account));
                    }
                    (_, _) => {}
                }
            });

        accounts
    }

    /// iterate over all stakes, redeem vote credits for each stake we can
    ///   successfully load and parse, return the lamport value of one point
    fn pay_validator_rewards(
        &mut self,
        rewarded_epoch: Epoch,
        rewards: u64,
        reward_calc_tracer: &mut Option<impl FnMut(&RewardCalculationEvent)>,
        fix_stake_deactivate: bool,
    ) -> f64 {
        let stake_history = self.stakes.read().unwrap().history().clone();

        let mut stake_delegation_accounts = self.stake_delegation_accounts(reward_calc_tracer);

        let points: u128 = stake_delegation_accounts
            .iter()
            .flat_map(|(_vote_pubkey, (stake_group, vote_account))| {
                stake_group
                    .iter()
                    .map(move |(_stake_pubkey, stake_account)| (stake_account, vote_account))
            })
            .map(|(stake_account, vote_account)| {
                stake_state::calculate_points(
                    &stake_account,
                    &vote_account,
                    Some(&stake_history),
                    fix_stake_deactivate,
                )
                .unwrap_or(0)
            })
            .sum();

        if points == 0 {
            return 0.0;
        }

        let point_value = PointValue { rewards, points };

        let mut rewards = vec![];
        // pay according to point value
        for (vote_pubkey, (stake_group, vote_account)) in stake_delegation_accounts.iter_mut() {
            let mut vote_account_changed = false;
            let voters_account_pre_balance = vote_account.lamports();

            for (stake_pubkey, stake_account) in stake_group.iter_mut() {
                // curry closure to add the contextual stake_pubkey
                let mut reward_calc_tracer = reward_calc_tracer.as_mut().map(|outer| {
                    let stake_pubkey = *stake_pubkey;
                    // inner
                    move |inner_event: &_| {
                        outer(&RewardCalculationEvent::Staking(&stake_pubkey, inner_event))
                    }
                });
                let redeemed = stake_state::redeem_rewards(
                    rewarded_epoch,
                    stake_account,
                    vote_account,
                    &point_value,
                    Some(&stake_history),
                    &mut reward_calc_tracer.as_mut(),
                    fix_stake_deactivate,
                );
                if let Ok((stakers_reward, _voters_reward)) = redeemed {
                    self.store_account(&stake_pubkey, &stake_account);
                    vote_account_changed = true;

                    if stakers_reward > 0 {
                        rewards.push((
                            *stake_pubkey,
                            RewardInfo {
                                reward_type: RewardType::Staking,
                                lamports: stakers_reward as i64,
                                post_balance: stake_account.lamports(),
                            },
                        ));
                    }
                } else {
                    debug!(
                        "stake_state::redeem_rewards() failed for {}: {:?}",
                        stake_pubkey, redeemed
                    );
                }
            }

            if vote_account_changed {
                let post_balance = vote_account.lamports();
                let lamports = (post_balance - voters_account_pre_balance) as i64;
                if lamports != 0 {
                    rewards.push((
                        *vote_pubkey,
                        RewardInfo {
                            reward_type: RewardType::Voting,
                            lamports,
                            post_balance,
                        },
                    ));
                }
                self.store_account(&vote_pubkey, &vote_account);
            }
        }
        self.rewards.write().unwrap().append(&mut rewards);

        point_value.rewards as f64 / point_value.points as f64
    }

    fn update_recent_blockhashes_locked(&self, locked_blockhash_queue: &BlockhashQueue) {
        self.update_sysvar_account(&sysvar::recent_blockhashes::id(), |account| {
            let recent_blockhash_iter = locked_blockhash_queue.get_recent_blockhashes();
            recent_blockhashes_account::create_account_with_data_and_fields(
                recent_blockhash_iter,
                self.inherit_specially_retained_account_fields(account),
            )
        });
    }

    pub fn update_recent_blockhashes(&self) {
        let blockhash_queue = self.blockhash_queue.read().unwrap();
        self.update_recent_blockhashes_locked(&blockhash_queue);
    }

    fn get_timestamp_estimate(
        &self,
        max_allowable_drift: MaxAllowableDrift,
        epoch_start_timestamp: Option<(Slot, UnixTimestamp)>,
    ) -> Option<UnixTimestamp> {
        let mut get_timestamp_estimate_time = Measure::start("get_timestamp_estimate");
        let slots_per_epoch = self.epoch_schedule().slots_per_epoch;
        let recent_timestamps =
            self.vote_accounts()
                .into_iter()
                .filter_map(|(pubkey, (_, account))| {
                    let vote_state = account.vote_state();
                    let vote_state = vote_state.as_ref().ok()?;
                    let slot_delta = self.slot().checked_sub(vote_state.last_timestamp.slot)?;
                    if slot_delta <= slots_per_epoch {
                        Some((
                            pubkey,
                            (
                                vote_state.last_timestamp.slot,
                                vote_state.last_timestamp.timestamp,
                            ),
                        ))
                    } else {
                        None
                    }
                });
        let slot_duration = Duration::from_nanos(self.ns_per_slot as u64);
        let epoch = self.epoch_schedule().get_epoch(self.slot());
        let stakes = self.epoch_vote_accounts(epoch)?;
        let stake_weighted_timestamp = calculate_stake_weighted_timestamp(
            recent_timestamps,
            stakes,
            self.slot(),
            slot_duration,
            epoch_start_timestamp,
            max_allowable_drift,
            self.feature_set
                .is_active(&feature_set::warp_timestamp_again::id()),
        );
        get_timestamp_estimate_time.stop();
        datapoint_info!(
            "bank-timestamp",
            (
                "get_timestamp_estimate_us",
                get_timestamp_estimate_time.as_us(),
                i64
            ),
        );
        stake_weighted_timestamp
    }

    // Distribute collected transaction fees for this slot to collector_id (= current leader).
    //
    // Each validator is incentivized to process more transactions to earn more transaction fees.
    // Transaction fees are rewarded for the computing resource utilization cost, directly
    // proportional to their actual processing power.
    //
    // collector_id is rotated according to stake-weighted leader schedule. So the opportunity of
    // earning transaction fees are fairly distributed by stake. And missing the opportunity
    // (not producing a block as a leader) earns nothing. So, being online is incentivized as a
    // form of transaction fees as well.
    //
    // On the other hand, rent fees are distributed under slightly different philosophy, while
    // still being stake-weighted.
    // Ref: distribute_rent_to_validators
    fn collect_fees(&self) {
        let collector_fees = self.collector_fees.load(Relaxed) as u64;

        if collector_fees != 0 {
            let (deposit, mut burn) = self.fee_rate_governor.burn(collector_fees);
            // burn a portion of fees
            debug!(
                "distributed fee: {} (rounded from: {}, burned: {})",
                deposit, collector_fees, burn
            );

            match self.deposit(&self.collector_id, deposit) {
                Ok(post_balance) => {
                    if deposit != 0 {
                        self.rewards.write().unwrap().push((
                            self.collector_id,
                            RewardInfo {
                                reward_type: RewardType::Fee,
                                lamports: deposit as i64,
                                post_balance,
                            },
                        ));
                    }
                }
                Err(_) => {
                    error!(
                        "Burning {} fee instead of crediting {}",
                        deposit, self.collector_id
                    );
                    inc_new_counter_error!("bank-burned_fee_lamports", deposit as usize);
                    burn += deposit;
                }
            }
            self.capitalization.fetch_sub(burn, Relaxed);
        }
    }

    pub fn rehash(&self) {
        let mut hash = self.hash.write().unwrap();
        let new = self.hash_internal_state();
        if new != *hash {
            warn!("Updating bank hash to {}", new);
            *hash = new;
        }
    }

    pub fn epoch_schedule(&self) -> &EpochSchedule {
        &self.epoch_schedule
    }

    /// Return the more recent checkpoint of this bank instance.
    pub fn parent(&self) -> Option<Arc<Bank>> {
        self.rc.parent.read().unwrap().clone()
    }

    pub fn parent_slot(&self) -> Slot {
        self.parent_slot
    }

    pub fn parent_hash(&self) -> Hash {
        self.parent_hash
    }

    pub fn set_rent_burn_percentage(&mut self, burn_percent: u8) {
        self.rent_collector.rent.burn_percent = burn_percent;
    }

    pub fn set_hashes_per_tick(&mut self, hashes_per_tick: Option<u64>) {
        self.hashes_per_tick = hashes_per_tick;
    }

    /// Return the last block hash registered.
    pub fn last_blockhash(&self) -> Hash {
        self.blockhash_queue.read().unwrap().last_hash()
    }

    pub fn get_minimum_balance_for_rent_exemption(&self, data_len: usize) -> u64 {
        self.rent_collector.rent.minimum_balance(data_len)
    }

    pub fn last_blockhash_with_fee_calculator(&self) -> (Hash, FeeCalculator) {
        let blockhash_queue = self.blockhash_queue.read().unwrap();
        let last_hash = blockhash_queue.last_hash();
        (
            last_hash,
            blockhash_queue
                .get_fee_calculator(&last_hash)
                .unwrap()
                .clone(),
        )
    }

    pub fn get_fee_calculator(&self, hash: &Hash) -> Option<FeeCalculator> {
        let blockhash_queue = self.blockhash_queue.read().unwrap();
        blockhash_queue.get_fee_calculator(hash).cloned()
    }

    pub fn get_fee_rate_governor(&self) -> &FeeRateGovernor {
        &self.fee_rate_governor
    }

    pub fn get_blockhash_last_valid_slot(&self, blockhash: &Hash) -> Option<Slot> {
        let blockhash_queue = self.blockhash_queue.read().unwrap();
        // This calculation will need to be updated to consider epoch boundaries if BlockhashQueue
        // length is made variable by epoch
        blockhash_queue
            .get_hash_age(blockhash)
            .map(|age| self.slot + blockhash_queue.len() as u64 - age)
    }

    pub fn confirmed_last_blockhash(&self) -> (Hash, FeeCalculator) {
        const NUM_BLOCKHASH_CONFIRMATIONS: usize = 3;

        let parents = self.parents();
        if parents.is_empty() {
            self.last_blockhash_with_fee_calculator()
        } else {
            let index = NUM_BLOCKHASH_CONFIRMATIONS.min(parents.len() - 1);
            parents[index].last_blockhash_with_fee_calculator()
        }
    }

    /// Forget all signatures. Useful for benchmarking.
    pub fn clear_signatures(&self) {
        self.src.status_cache.write().unwrap().clear();
    }

    pub fn clear_slot_signatures(&self, slot: Slot) {
        self.src
            .status_cache
            .write()
            .unwrap()
            .clear_slot_entries(slot);
    }

    pub fn can_commit(result: &Result<()>) -> bool {
        match result {
            Ok(_) => true,
            Err(TransactionError::InstructionError(_, _)) => true,
            Err(_) => false,
        }
    }

    /// Tell the bank which Entry IDs exist on the ledger. This function
    /// assumes subsequent calls correspond to later entries, and will boot
    /// the oldest ones once its internal cache is full. Once boot, the
    /// bank will reject transactions using that `hash`.
    pub fn register_tick(&self, hash: &Hash) {
        assert!(
            !self.freeze_started(),
            "register_tick() working on a bank that is already frozen or is undergoing freezing!"
        );

        inc_new_counter_debug!("bank-register_tick-registered", 1);
        let mut w_blockhash_queue = self.blockhash_queue.write().unwrap();
        if self.is_block_boundary(self.tick_height.load(Relaxed) + 1) {
            w_blockhash_queue.register_hash(hash, &self.fee_calculator);
            if self.fix_recent_blockhashes_sysvar_delay() {
                self.update_recent_blockhashes_locked(&w_blockhash_queue);
            }
        }
        // ReplayStage will start computing the accounts delta hash when it
        // detects the tick height has reached the boundary, so the system
        // needs to guarantee all account updates for the slot have been
        // committed before this tick height is incremented (like the blockhash
        // sysvar above)
        self.tick_height.fetch_add(1, Relaxed);
    }

    pub fn is_complete(&self) -> bool {
        self.tick_height() == self.max_tick_height()
    }

    pub fn is_block_boundary(&self, tick_height: u64) -> bool {
        tick_height % self.ticks_per_slot == 0
    }

    pub fn demote_sysvar_write_locks(&self) -> bool {
        self.feature_set
            .is_active(&feature_set::demote_sysvar_write_locks::id())
    }

    pub fn unlock_accounts(&self, batch: &mut TransactionBatch) {
        if batch.needs_unlock {
            batch.needs_unlock = false;
            self.rc.accounts.unlock_accounts(
                batch.transactions_iter(),
                batch.lock_results(),
                self.demote_sysvar_write_locks(),
            )
        }
    }

    pub fn remove_unrooted_slot(&self, slot: Slot) {
        self.rc.accounts.accounts_db.remove_unrooted_slot(slot)
    }

    pub fn set_shrink_paths(&self, paths: Vec<PathBuf>) {
        self.rc.accounts.accounts_db.set_shrink_paths(paths);
    }

    pub fn get_slot_history(&self) -> SlotHistory {
        from_account(&self.get_account(&sysvar::slot_history::id()).unwrap()).unwrap()
    }

    pub fn check_hash_age(&self, hash: &Hash, max_age: usize) -> Option<bool> {
        self.blockhash_queue
            .read()
            .unwrap()
            .check_hash_age(hash, max_age)
    }

    // Determine if the bank is currently in an upgrade epoch, where only votes are permitted
    fn upgrade_epoch(&self) -> bool {
        match self.cluster_type() {
            #[cfg(test)]
            ClusterType::Development => self.epoch == 0xdead, // Value assumed by `test_upgrade_epoch()`
            #[cfg(not(test))]
            ClusterType::Development => false,
            ClusterType::Devnet => false,
            ClusterType::Testnet => false,
            ClusterType::MainnetBeta => self.epoch == 61,
        }
    }

    pub fn collect_balances(&self, batch: &TransactionBatch) -> TransactionBalances {
        let mut balances: TransactionBalances = vec![];
        for transaction in batch.transactions_iter() {
            let mut transaction_balances: Vec<u64> = vec![];
            for account_key in transaction.message.account_keys.iter() {
                transaction_balances.push(self.get_balance(account_key));
            }
            balances.push(transaction_balances);
        }
        balances
    }

    #[allow(clippy::cognitive_complexity)]
    fn update_error_counters(error_counters: &ErrorCounters) {
        if 0 != error_counters.total {
            inc_new_counter_info!(
                "bank-process_transactions-error_count",
                error_counters.total
            );
        }
        if 0 != error_counters.account_not_found {
            inc_new_counter_info!(
                "bank-process_transactions-account_not_found",
                error_counters.account_not_found
            );
        }
        if 0 != error_counters.account_in_use {
            inc_new_counter_info!(
                "bank-process_transactions-account_in_use",
                error_counters.account_in_use
            );
        }
        if 0 != error_counters.account_loaded_twice {
            inc_new_counter_info!(
                "bank-process_transactions-account_loaded_twice",
                error_counters.account_loaded_twice
            );
        }
        if 0 != error_counters.blockhash_not_found {
            inc_new_counter_info!(
                "bank-process_transactions-error-blockhash_not_found",
                error_counters.blockhash_not_found
            );
        }
        if 0 != error_counters.blockhash_too_old {
            inc_new_counter_info!(
                "bank-process_transactions-error-blockhash_too_old",
                error_counters.blockhash_too_old
            );
        }
        if 0 != error_counters.invalid_account_index {
            inc_new_counter_info!(
                "bank-process_transactions-error-invalid_account_index",
                error_counters.invalid_account_index
            );
        }
        if 0 != error_counters.invalid_account_for_fee {
            inc_new_counter_info!(
                "bank-process_transactions-error-invalid_account_for_fee",
                error_counters.invalid_account_for_fee
            );
        }
        if 0 != error_counters.insufficient_funds {
            inc_new_counter_info!(
                "bank-process_transactions-error-insufficient_funds",
                error_counters.insufficient_funds
            );
        }
        if 0 != error_counters.instruction_error {
            inc_new_counter_info!(
                "bank-process_transactions-error-instruction_error",
                error_counters.instruction_error
            );
        }
        if 0 != error_counters.already_processed {
            inc_new_counter_info!(
                "bank-process_transactions-error-already_processed",
                error_counters.already_processed
            );
        }
        if 0 != error_counters.not_allowed_during_cluster_maintenance {
            inc_new_counter_info!(
                "bank-process_transactions-error-cluster-maintenance",
                error_counters.not_allowed_during_cluster_maintenance
            );
        }
    }

    fn compile_recorded_instructions(
        inner_instructions: &mut Vec<Option<InnerInstructionsList>>,
        instruction_recorders: Option<Vec<InstructionRecorder>>,
        message: &Message,
    ) {
        inner_instructions.push(instruction_recorders.map(|instruction_recorders| {
            instruction_recorders
                .into_iter()
                .map(|r| r.compile_instructions(message))
                .collect()
        }));
    }

    fn run_incinerator(&self) {
        if let Some((account, _)) =
            self.get_account_modified_since_parent_with_fixed_root(&incinerator::id())
        {
            self.capitalization.fetch_sub(account.lamports(), Relaxed);
            self.store_account(&incinerator::id(), &AccountSharedData::default());
        }
    }

    #[cfg(test)]
    fn restore_old_behavior_for_fragile_tests(&self) {
        self.lazy_rent_collection.store(true, Relaxed);
        self.no_stake_rewrite.store(true, Relaxed);
    }

    fn enable_eager_rent_collection(&self) -> bool {
        if self.lazy_rent_collection.load(Relaxed) {
            return false;
        }

        true
    }

    pub fn cluster_type(&self) -> ClusterType {
        // unwrap is safe; self.cluster_type is ensured to be Some() always...
        // we only using Option here for ABI compatibility...
        self.cluster_type.unwrap()
    }

    /// Compute all the parents of the bank in order
    pub fn parents(&self) -> Vec<Arc<Bank>> {
        let mut parents = vec![];
        let mut bank = self.parent();
        while let Some(parent) = bank {
            parents.push(parent.clone());
            bank = parent.parent();
        }
        parents
    }

    /// Compute all the parents of the bank including this bank itself
    pub fn parents_inclusive(self: Arc<Self>) -> Vec<Arc<Bank>> {
        let mut parents = self.parents();
        parents.insert(0, self);
        parents
    }

    pub fn expire_old_recycle_stores(&self) {
        self.rc.accounts.accounts_db.expire_old_recycle_stores()
    }

    pub fn set_inflation(&self, inflation: Inflation) {
        *self.inflation.write().unwrap() = inflation;
    }

    pub fn set_bpf_compute_budget(&mut self, bpf_compute_budget: Option<BpfComputeBudget>) {
        self.bpf_compute_budget = bpf_compute_budget;
    }

    pub fn hard_forks(&self) -> Arc<RwLock<HardForks>> {
        self.hard_forks.clone()
    }

    /// Hash the `accounts` HashMap. This represents a validator's interpretation
    ///  of the delta of the ledger since the last vote and up to now
    fn hash_internal_state(&self) -> Hash {
        // If there are no accounts, return the hash of the previous state and the latest blockhash
        let accounts_delta_hash = self.rc.accounts.bank_hash_info_at(self.slot());
        let mut signature_count_buf = [0u8; 8];
        LittleEndian::write_u64(&mut signature_count_buf[..], self.signature_count() as u64);

        let mut hash = hashv(&[
            self.parent_hash.as_ref(),
            accounts_delta_hash.hash.as_ref(),
            &signature_count_buf,
            self.last_blockhash().as_ref(),
        ]);

        if let Some(buf) = self
            .hard_forks
            .read()
            .unwrap()
            .get_hash_data(self.slot(), self.parent_slot())
        {
            info!("hard fork at bank {}", self.slot());
            hash = extend_and_hash(&hash, &buf)
        }

        info!(
            "bank frozen: {} hash: {} accounts_delta: {} signature_count: {} last_blockhash: {} capitalization: {}",
            self.slot(),
            hash,
            accounts_delta_hash.hash,
            self.signature_count(),
            self.last_blockhash(),
            self.capitalization(),
        );

        info!(
            "accounts hash slot: {} stats: {:?}",
            self.slot(),
            accounts_delta_hash.stats,
        );
        hash
    }

    /// Recalculate the hash_internal_state from the account stores. Would be used to verify a
    /// snapshot.
    #[must_use]
    fn verify_bank_hash(&self) -> bool {
        self.rc.accounts.verify_bank_hash_and_lamports(
            self.slot(),
            &self.ancestors,
            self.capitalization(),
        )
    }

    pub fn get_snapshot_storages(&self) -> SnapshotStorages {
        self.rc
            .get_snapshot_storages(self.slot())
            .into_iter()
            .collect()
    }

    #[must_use]
    fn verify_hash(&self) -> bool {
        assert!(self.is_frozen());
        let calculated_hash = self.hash_internal_state();
        let expected_hash = self.hash();

        if calculated_hash == expected_hash {
            true
        } else {
            warn!(
                "verify failed: slot: {}, {} (calculated) != {} (expected)",
                self.slot(),
                calculated_hash,
                expected_hash
            );
            false
        }
    }

    pub fn get_accounts_hash(&self) -> Hash {
        self.rc.accounts.accounts_db.get_accounts_hash(self.slot)
    }

    pub fn get_thread_pool(&self) -> &ThreadPool {
        &self.rc.accounts.accounts_db.thread_pool_clean
    }

    pub fn update_accounts_hash_with_index_option(
        &self,
        use_index: bool,
        debug_verify: bool,
    ) -> Hash {
        let (hash, total_lamports) = self
            .rc
            .accounts
            .accounts_db
            .update_accounts_hash_with_index_option(
                use_index,
                debug_verify,
                self.slot(),
                &self.ancestors,
                Some(self.capitalization()),
            );
        assert_eq!(total_lamports, self.capitalization());
        hash
    }

    pub fn update_accounts_hash(&self) -> Hash {
        self.update_accounts_hash_with_index_option(true, false)
    }

    /// A snapshot bank should be purged of 0 lamport accounts which are not part of the hash
    /// calculation and could shield other real accounts.
    pub fn verify_snapshot_bank(&self) -> bool {
        if self.slot() > 0 {
            self.clean_accounts(true);
            self.shrink_all_slots();
        }
        // Order and short-circuiting is significant; verify_hash requires a valid bank hash
        self.verify_bank_hash() && self.verify_hash()
    }

    /// Return the inflation parameters of the Bank
    pub fn inflation(&self) -> Inflation {
        *self.inflation.read().unwrap()
    }

    /// returns the epoch for which this bank's leader_schedule_slot_offset and slot would
    ///  need to cache leader_schedule
    pub fn get_leader_schedule_epoch(&self, slot: Slot) -> Epoch {
        self.epoch_schedule.get_leader_schedule_epoch(slot)
    }

    /// current stake delegations for this bank
    pub fn cloned_stake_delegations(&self) -> HashMap<Pubkey, Delegation> {
        self.stakes.read().unwrap().stake_delegations().clone()
    }

    pub fn staked_nodes(&self) -> HashMap<Pubkey, u64> {
        self.stakes.read().unwrap().staked_nodes()
    }

    /// Get the EpochStakes for a given epoch
    pub fn epoch_stakes(&self, epoch: Epoch) -> Option<&EpochStakes> {
        self.epoch_stakes.get(&epoch)
    }

    pub fn epoch_stakes_map(&self) -> &HashMap<Epoch, EpochStakes> {
        &self.epoch_stakes
    }

    pub fn epoch_staked_nodes(&self, epoch: Epoch) -> Option<HashMap<Pubkey, u64>> {
        Some(self.epoch_stakes.get(&epoch)?.stakes().staked_nodes())
    }

    /// vote accounts for the specific epoch along with the stake
    ///   attributed to each account
    pub fn epoch_vote_accounts(
        &self,
        epoch: Epoch,
    ) -> Option<&HashMap<Pubkey, (u64, ArcVoteAccount)>> {
        self.epoch_stakes
            .get(&epoch)
            .map(|epoch_stakes| Stakes::vote_accounts(epoch_stakes.stakes()))
    }

    /// Get the fixed authorized voter for the given vote account for the
    /// current epoch
    pub fn epoch_authorized_voter(&self, vote_account: &Pubkey) -> Option<&Pubkey> {
        self.epoch_stakes
            .get(&self.epoch)
            .expect("Epoch stakes for bank's own epoch must exist")
            .epoch_authorized_voters()
            .get(vote_account)
    }

    /// Get the fixed set of vote accounts for the given node id for the
    /// current epoch
    pub fn epoch_vote_accounts_for_node_id(&self, node_id: &Pubkey) -> Option<&NodeVoteAccounts> {
        self.epoch_stakes
            .get(&self.epoch)
            .expect("Epoch stakes for bank's own epoch must exist")
            .node_id_to_vote_accounts()
            .get(node_id)
    }

    /// Get the fixed total stake of all vote accounts for current epoch
    pub fn total_epoch_stake(&self) -> u64 {
        self.epoch_stakes
            .get(&self.epoch)
            .expect("Epoch stakes for bank's own epoch must exist")
            .total_stake()
    }

    /// Get the fixed stake of the given vote account for the current epoch
    pub fn epoch_vote_account_stake(&self, vote_account: &Pubkey) -> u64 {
        *self
            .epoch_vote_accounts(self.epoch())
            .expect("Bank epoch vote accounts must contain entry for the bank's own epoch")
            .get(vote_account)
            .map(|(stake, _)| stake)
            .unwrap_or(&0)
    }

    /// given a slot, return the epoch and offset into the epoch this slot falls
    /// e.g. with a fixed number for slots_per_epoch, the calculation is simply:
    ///
    ///  ( slot/slots_per_epoch, slot % slots_per_epoch )
    ///
    pub fn get_epoch_and_slot_index(&self, slot: Slot) -> (Epoch, SlotIndex) {
        self.epoch_schedule.get_epoch_and_slot_index(slot)
    }

    pub fn get_epoch_info(&self) -> EpochInfo {
        let absolute_slot = self.slot();
        let block_height = self.block_height();
        let (epoch, slot_index) = self.get_epoch_and_slot_index(absolute_slot);
        let slots_in_epoch = self.get_slots_in_epoch(epoch);
        let transaction_count = Some(self.transaction_count());
        EpochInfo {
            epoch,
            slot_index,
            slots_in_epoch,
            absolute_slot,
            block_height,
            transaction_count,
        }
    }

    pub fn is_empty(&self) -> bool {
        !self.is_delta.load(Relaxed)
    }

    pub fn shrink_all_slots(&self) {
        self.rc.accounts.accounts_db.shrink_all_slots();
    }

    pub fn print_accounts_stats(&self) {
        self.rc.accounts.accounts_db.print_accounts_stats("");
    }

    pub fn process_stale_slot_with_budget(
        &self,
        mut consumed_budget: usize,
        budget_recovery_delta: usize,
    ) -> usize {
        if consumed_budget == 0 {
            let shrunken_account_count = self.rc.accounts.accounts_db.process_stale_slot_v1();
            if shrunken_account_count > 0 {
                datapoint_info!(
                    "stale_slot_shrink",
                    ("accounts", shrunken_account_count, i64)
                );
                consumed_budget += shrunken_account_count;
            }
        }
        consumed_budget.saturating_sub(budget_recovery_delta)
    }

    pub fn shrink_candidate_slots(&self) -> usize {
        self.rc.accounts.accounts_db.shrink_candidate_slots()
    }

    fn apply_spl_token_v2_self_transfer_fix(&mut self) {
        if let Some(old_account) = self.get_account_with_fixed_root(&inline_spl_token_v2_0::id()) {
            if let Some(new_account) =
                self.get_account_with_fixed_root(&inline_spl_token_v2_0::new_token_program::id())
            {
                datapoint_info!(
                    "bank-apply_spl_token_v2_self_transfer_fix",
                    ("slot", self.slot, i64),
                );

                // Burn lamports in the old token account
                self.capitalization
                    .fetch_sub(old_account.lamports(), Relaxed);

                // Transfer new token account to old token account
                self.store_account(&inline_spl_token_v2_0::id(), &new_account);

                // Clear new token account
                self.store_account(
                    &inline_spl_token_v2_0::new_token_program::id(),
                    &AccountSharedData::default(),
                );

                self.remove_executor(&inline_spl_token_v2_0::id());
            }
        }
    }
    fn reconfigure_token2_native_mint(&mut self) {
        let reconfigure_token2_native_mint = match self.cluster_type() {
            ClusterType::Development => true,
            ClusterType::Devnet => true,
            ClusterType::Testnet => self.epoch() == 93,
            ClusterType::MainnetBeta => self.epoch() == 75,
        };

        if reconfigure_token2_native_mint {
            let mut native_mint_account = solana_sdk::account::AccountSharedData::from(Account {
                owner: inline_spl_token_v2_0::id(),
                data: inline_spl_token_v2_0::native_mint::ACCOUNT_DATA.to_vec(),
                lamports: sol_to_lamports(1.),
                executable: false,
                rent_epoch: self.epoch() + 1,
            });

            // As a workaround for
            // https://github.com/solana-labs/solana-program-library/issues/374, ensure that the
            // spl-token 2 native mint account is owned by the spl-token 2 program.
            let store = if let Some(existing_native_mint_account) =
                self.get_account_with_fixed_root(&inline_spl_token_v2_0::native_mint::id())
            {
                if existing_native_mint_account.owner() == &solana_sdk::system_program::id() {
                    native_mint_account.set_lamports(existing_native_mint_account.lamports());
                    true
                } else {
                    false
                }
            } else {
                self.capitalization
                    .fetch_add(native_mint_account.lamports(), Relaxed);
                true
            };

            if store {
                self.store_account(
                    &inline_spl_token_v2_0::native_mint::id(),
                    &native_mint_account,
                );
            }
        }
    }

    fn ensure_no_storage_rewards_pool(&mut self) {
        let purge_window_epoch = match self.cluster_type() {
            ClusterType::Development => false,
            // never do this for devnet; we're pristine here. :)
            ClusterType::Devnet => false,
            // schedule to remove at testnet/tds
            ClusterType::Testnet => self.epoch() == 93,
            // never do this for stable; we're pristine here. :)
            ClusterType::MainnetBeta => false,
        };

        if purge_window_epoch {
            for reward_pubkey in self.rewards_pool_pubkeys.iter() {
                if let Some(mut reward_account) = self.get_account_with_fixed_root(&reward_pubkey) {
                    if reward_account.lamports() == u64::MAX {
                        reward_account.set_lamports(0);
                        self.store_account(&reward_pubkey, &reward_account);
                        // Adjust capitalization.... it has been wrapping, reducing the real capitalization by 1-lamport
                        self.capitalization.fetch_add(1, Relaxed);
                        info!(
                            "purged rewards pool accont: {}, new capitalization: {}",
                            reward_pubkey,
                            self.capitalization()
                        );
                    }
                };
            }
        }
    }

    fn fix_recent_blockhashes_sysvar_delay(&self) -> bool {
        match self.cluster_type() {
            ClusterType::Development | ClusterType::Devnet | ClusterType::Testnet => true,
            ClusterType::MainnetBeta => self
                .feature_set
                .is_active(&feature_set::consistent_recent_blockhashes_sysvar::id()),
        }
    }
}

impl Drop for Bank {
    fn drop(&mut self) {
        if self.skip_drop.load(Relaxed) {
            return;
        }

        self.cleanup();

        if let Some(drop_callback) = self.drop_callback.read().unwrap().0.as_ref() {
            drop_callback.callback(self);
        } else {
            // Default case
            // 1. Tests
            // 2. At startup when replaying blockstore and there's no
            // AccountsBackgroundService to perform cleanups yet.
            self.rc.accounts.purge_slot(self.slot());
        }
    }
}

pub fn goto_end_of_slot(bank: &mut Bank) {
    let mut tick_hash = bank.last_blockhash();
    loop {
        tick_hash = hashv(&[&tick_hash.as_ref(), &[42]]);
        bank.register_tick(&tick_hash);
        if tick_hash == bank.last_blockhash() {
            bank.freeze();
            return;
        }
    }
}

fn is_simple_vote_transaction(transaction: &Transaction) -> bool {
    if transaction.message.instructions.len() == 1 {
        let instruction = &transaction.message.instructions[0];
        let program_pubkey =
            transaction.message.account_keys[instruction.program_id_index as usize];
        if program_pubkey == solana_vote_program::id() {
            if let Ok(vote_instruction) = limited_deserialize::<VoteInstruction>(&instruction.data)
            {
                return matches!(
                    vote_instruction,
                    VoteInstruction::Vote(_) | VoteInstruction::VoteSwitch(_, _)
                );
            }
        }
    }
    false
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{
        test_utils::{assert_capitalization_diff, update_vote_account_timestamp},
        *,
    };
    use crate::{
        accounts_db::SHRINK_RATIO,
        accounts_index::{AccountMap, Ancestors, ITER_BATCH_SIZE},
        genesis_utils::{
            activate_all_features, bootstrap_validator_stake_lamports,
            create_genesis_config_with_leader, create_genesis_config_with_vote_accounts,
            GenesisConfigInfo, ValidatorVoteKeypairs,
        },
        native_loader::NativeLoaderError,
        status_cache::MAX_CACHE_ENTRIES,
    };
    use crossbeam_channel::bounded;
    use solana_sdk::{
        account::{from_account, Account},
        account_utils::StateMut,
        clock::{DEFAULT_SLOTS_PER_EPOCH, DEFAULT_TICKS_PER_SLOT},
        epoch_schedule::MINIMUM_SLOTS_PER_EPOCH,
        feature::Feature,
        genesis_config::create_genesis_config,
        instruction::{AccountMeta, CompiledInstruction, Instruction, InstructionError},
        message::{Message, MessageHeader},
        nonce,
        poh_config::PohConfig,
        process_instruction::InvokeContext,
        rent::Rent,
        signature::{keypair_from_seed, Keypair, Signer},
        stake_weighted_timestamp::{
            MAX_ALLOWABLE_DRIFT_PERCENTAGE, MAX_ALLOWABLE_DRIFT_PERCENTAGE_FAST,
            MAX_ALLOWABLE_DRIFT_PERCENTAGE_SLOW,
        },
        system_instruction::{self, SystemError},
        system_program,
        sysvar::{fees::Fees, rewards::Rewards},
    };
    use solana_stake_program::{
        stake_instruction,
        stake_state::{self, Authorized, Delegation, Lockup, Stake},
    };
    use solana_vote_program::{
        vote_instruction,
        vote_state::{
            self, BlockTimestamp, Vote, VoteInit, VoteState, VoteStateVersions, MAX_LOCKOUT_HISTORY,
        },
    };
    use std::{result, thread::Builder, time::Duration};

    #[test]
    fn test_bank_block_height() {
        let (genesis_config, _mint_keypair) = create_genesis_config(1);
        let bank0 = Arc::new(Bank::new(&genesis_config));
        assert_eq!(bank0.block_height(), 0);
        let bank1 = Arc::new(new_from_parent(&bank0));
        assert_eq!(bank1.block_height(), 1);
    }

    #[test]
    fn test_bank_update_epoch_stakes() {
        impl Bank {
            fn epoch_stake_keys(&self) -> Vec<Epoch> {
                let mut keys: Vec<Epoch> = self.epoch_stakes.keys().copied().collect();
                keys.sort_unstable();
                keys
            }

            fn epoch_stake_key_info(&self) -> (Epoch, Epoch, usize) {
                let mut keys: Vec<Epoch> = self.epoch_stakes.keys().copied().collect();
                keys.sort_unstable();
                (*keys.first().unwrap(), *keys.last().unwrap(), keys.len())
            }
        }

        let (genesis_config, _mint_keypair) = create_genesis_config(100_000);
        let mut bank = Bank::new(&genesis_config);

        let initial_epochs = bank.epoch_stake_keys();
        assert_eq!(initial_epochs, vec![0, 1]);

        for existing_epoch in &initial_epochs {
            bank.update_epoch_stakes(*existing_epoch);
            assert_eq!(bank.epoch_stake_keys(), initial_epochs);
        }

        for epoch in (initial_epochs.len() as Epoch)..MAX_LEADER_SCHEDULE_STAKES {
            bank.update_epoch_stakes(epoch);
            assert_eq!(bank.epoch_stakes.len() as Epoch, epoch + 1);
        }

        assert_eq!(
            bank.epoch_stake_key_info(),
            (
                0,
                MAX_LEADER_SCHEDULE_STAKES - 1,
                MAX_LEADER_SCHEDULE_STAKES as usize
            )
        );

        bank.update_epoch_stakes(MAX_LEADER_SCHEDULE_STAKES);
        assert_eq!(
            bank.epoch_stake_key_info(),
            (
                0,
                MAX_LEADER_SCHEDULE_STAKES,
                MAX_LEADER_SCHEDULE_STAKES as usize + 1
            )
        );

        bank.update_epoch_stakes(MAX_LEADER_SCHEDULE_STAKES + 1);
        assert_eq!(
            bank.epoch_stake_key_info(),
            (
                1,
                MAX_LEADER_SCHEDULE_STAKES + 1,
                MAX_LEADER_SCHEDULE_STAKES as usize + 1
            )
        );
    }

    #[test]
    fn test_bank_capitalization() {
        let bank0 = Arc::new(Bank::new(&GenesisConfig {
            accounts: (0..42)
                .map(|_| {
                    (
                        solana_sdk::pubkey::new_rand(),
                        Account::new(42, 0, &Pubkey::default()),
                    )
                })
                .collect(),
            cluster_type: ClusterType::MainnetBeta,
            ..GenesisConfig::default()
        }));
        let sysvar_and_native_proram_delta0 = 10;
        assert_eq!(
            bank0.capitalization(),
            42 * 42 + sysvar_and_native_proram_delta0
        );
        let bank1 = Bank::new_from_parent(&bank0, &Pubkey::default(), 1);
        let sysvar_and_native_proram_delta1 = 2;
        assert_eq!(
            bank1.capitalization(),
            42 * 42 + sysvar_and_native_proram_delta0 + sysvar_and_native_proram_delta1,
        );
    }

    #[test]
    fn test_credit_debit_rent_no_side_effect_on_hash() {
        solana_logger::setup();

        let (mut genesis_config, _mint_keypair) = create_genesis_config(10);
        let keypair1: Keypair = Keypair::new();
        let keypair2: Keypair = Keypair::new();
        let keypair3: Keypair = Keypair::new();
        let keypair4: Keypair = Keypair::new();

        // Transaction between these two keypairs will fail
        let keypair5: Keypair = Keypair::new();
        let keypair6: Keypair = Keypair::new();

        genesis_config.rent = Rent {
            lamports_per_byte_year: 1,
            exemption_threshold: 21.0,
            burn_percent: 10,
        };

        let root_bank = Arc::new(Bank::new(&genesis_config));
        let bank = Bank::new_from_parent(
            &root_bank,
            &Pubkey::default(),
            years_as_slots(
                2.0,
                &genesis_config.poh_config.target_tick_duration,
                genesis_config.ticks_per_slot,
            ) as u64,
        );

        let root_bank_2 = Arc::new(Bank::new(&genesis_config));
        let bank_with_success_txs = Bank::new_from_parent(
            &root_bank_2,
            &Pubkey::default(),
            years_as_slots(
                2.0,
                &genesis_config.poh_config.target_tick_duration,
                genesis_config.ticks_per_slot,
            ) as u64,
        );

        assert_eq!(bank.last_blockhash(), genesis_config.hash());

        // Initialize credit-debit and credit only accounts
        let account1 = AccountSharedData::new(264, 0, &Pubkey::default());
        let account2 = AccountSharedData::new(264, 1, &Pubkey::default());
        let account3 = AccountSharedData::new(264, 0, &Pubkey::default());
        let account4 = AccountSharedData::new(264, 1, &Pubkey::default());
        let account5 = AccountSharedData::new(10, 0, &Pubkey::default());
        let account6 = AccountSharedData::new(10, 1, &Pubkey::default());

        bank.store_account(&keypair1.pubkey(), &account1);
        bank.store_account(&keypair2.pubkey(), &account2);
        bank.store_account(&keypair3.pubkey(), &account3);
        bank.store_account(&keypair4.pubkey(), &account4);
        bank.store_account(&keypair5.pubkey(), &account5);
        bank.store_account(&keypair6.pubkey(), &account6);

        bank_with_success_txs.store_account(&keypair1.pubkey(), &account1);
        bank_with_success_txs.store_account(&keypair2.pubkey(), &account2);
        bank_with_success_txs.store_account(&keypair3.pubkey(), &account3);
        bank_with_success_txs.store_account(&keypair4.pubkey(), &account4);
        bank_with_success_txs.store_account(&keypair5.pubkey(), &account5);
        bank_with_success_txs.store_account(&keypair6.pubkey(), &account6);

        // Make native instruction loader rent exempt
        let system_program_id = system_program::id();
        let mut system_program_account = bank.get_account(&system_program_id).unwrap();
        system_program_account.set_lamports(
            bank.get_minimum_balance_for_rent_exemption(system_program_account.data().len()),
        );
        bank.store_account(&system_program_id, &system_program_account);
        bank_with_success_txs.store_account(&system_program_id, &system_program_account);

        let t1 =
            system_transaction::transfer(&keypair1, &keypair2.pubkey(), 1, genesis_config.hash());
        let t2 =
            system_transaction::transfer(&keypair3, &keypair4.pubkey(), 1, genesis_config.hash());
        let t3 =
            system_transaction::transfer(&keypair5, &keypair6.pubkey(), 1, genesis_config.hash());

        let res = bank.process_transactions(&[t1.clone(), t2.clone(), t3]);

        assert_eq!(res.len(), 3);
        assert_eq!(res[0], Ok(()));
        assert_eq!(res[1], Ok(()));
        assert_eq!(res[2], Err(TransactionError::AccountNotFound));

        bank.freeze();

        let rwlockguard_bank_hash = bank.hash.read().unwrap();
        let bank_hash = rwlockguard_bank_hash.as_ref();

        let res = bank_with_success_txs.process_transactions(&[t2, t1]);

        assert_eq!(res.len(), 2);
        assert_eq!(res[0], Ok(()));
        assert_eq!(res[1], Ok(()));

        bank_with_success_txs.freeze();

        let rwlockguard_bank_with_success_txs_hash = bank_with_success_txs.hash.read().unwrap();
        let bank_with_success_txs_hash = rwlockguard_bank_with_success_txs_hash.as_ref();

        assert_eq!(bank_with_success_txs_hash, bank_hash);
    }

    #[test]
    fn test_bank_update_vote_stake_rewards() {
        solana_logger::setup();

        // create a bank that ticks really slowly...
        let bank0 = Arc::new(Bank::new(&GenesisConfig {
            accounts: (0..42)
                .map(|_| {
                    (
                        solana_sdk::pubkey::new_rand(),
                        Account::new(1_000_000_000, 0, &Pubkey::default()),
                    )
                })
                .collect(),
            // set it up so the first epoch is a full year long
            poh_config: PohConfig {
                target_tick_duration: Duration::from_secs(
                    SECONDS_PER_YEAR as u64
                        / MINIMUM_SLOTS_PER_EPOCH as u64
                        / DEFAULT_TICKS_PER_SLOT,
                ),
                hashes_per_tick: None,
                target_tick_count: None,
            },
            cluster_type: ClusterType::MainnetBeta,

            ..GenesisConfig::default()
        }));

        // enable lazy rent collection because this test depends on rent-due accounts
        // not being eagerly-collected for exact rewards calculation
        bank0.restore_old_behavior_for_fragile_tests();

        let sysvar_and_native_proram_delta0 = 10;
        assert_eq!(
            bank0.capitalization(),
            42 * 1_000_000_000 + sysvar_and_native_proram_delta0
        );
        assert!(bank0.rewards.read().unwrap().is_empty());

        let ((vote_id, mut vote_account), (stake_id, stake_account)) =
            crate::stakes::tests::create_staked_node_accounts(1_0000);

        // set up accounts
        bank0.store_account_and_update_capitalization(&stake_id, &stake_account);

        // generate some rewards
        let mut vote_state = Some(VoteState::from(&vote_account).unwrap());
        for i in 0..MAX_LOCKOUT_HISTORY + 42 {
            if let Some(v) = vote_state.as_mut() {
                v.process_slot_vote_unchecked(i as u64)
            }
            let versioned = VoteStateVersions::Current(Box::new(vote_state.take().unwrap()));
            VoteState::to(&versioned, &mut vote_account).unwrap();
            bank0.store_account_and_update_capitalization(&vote_id, &vote_account);
            match versioned {
                VoteStateVersions::Current(v) => {
                    vote_state = Some(*v);
                }
                _ => panic!("Has to be of type Current"),
            };
        }
        bank0.store_account_and_update_capitalization(&vote_id, &vote_account);

        let validator_points: u128 = bank0
            .stake_delegation_accounts(&mut null_tracer())
            .iter()
            .flat_map(|(_vote_pubkey, (stake_group, vote_account))| {
                stake_group
                    .iter()
                    .map(move |(_stake_pubkey, stake_account)| (stake_account, vote_account))
            })
            .map(|(stake_account, vote_account)| {
                stake_state::calculate_points(&stake_account, &vote_account, None, true)
                    .unwrap_or(0)
            })
            .sum();

        // put a child bank in epoch 1, which calls update_rewards()...
        let bank1 = Bank::new_from_parent(
            &bank0,
            &Pubkey::default(),
            bank0.get_slots_in_epoch(bank0.epoch()) + 1,
        );
        // verify that there's inflation
        assert_ne!(bank1.capitalization(), bank0.capitalization());

        // verify the inflation is represented in validator_points *
        let sysvar_and_native_proram_delta1 = 2;
        let paid_rewards =
            bank1.capitalization() - bank0.capitalization() - sysvar_and_native_proram_delta1;

        let rewards = bank1
            .get_account(&sysvar::rewards::id())
            .map(|account| from_account::<Rewards, _>(&account).unwrap())
            .unwrap();

        // verify the stake and vote accounts are the right size
        assert!(
            ((bank1.get_balance(&stake_id) - stake_account.lamports() + bank1.get_balance(&vote_id)
                - vote_account.lamports()) as f64
                - rewards.validator_point_value * validator_points as f64)
                .abs()
                < 1.0
        );

        // verify the rewards are the right size
        let allocated_rewards = rewards.validator_point_value * validator_points as f64;
        assert!((allocated_rewards - paid_rewards as f64).abs() < 1.0); // rounding, truncating

        // verify validator rewards show up in bank1.rewards vector
        assert_eq!(
            *bank1.rewards.read().unwrap(),
            vec![(
                stake_id,
                RewardInfo {
                    reward_type: RewardType::Staking,
                    lamports: (rewards.validator_point_value * validator_points as f64) as i64,
                    post_balance: bank1.get_balance(&stake_id),
                }
            )]
        );
        bank1.freeze();
        assert!(bank1.calculate_and_verify_capitalization());
    }

    fn do_test_bank_update_rewards_determinism() -> u64 {
        // create a bank that ticks really slowly...
        let bank = Arc::new(Bank::new(&GenesisConfig {
            accounts: (0..42)
                .map(|_| {
                    (
                        solana_sdk::pubkey::new_rand(),
                        Account::new(1_000_000_000, 0, &Pubkey::default()),
                    )
                })
                .collect(),
            // set it up so the first epoch is a full year long
            poh_config: PohConfig {
                target_tick_duration: Duration::from_secs(
                    SECONDS_PER_YEAR as u64
                        / MINIMUM_SLOTS_PER_EPOCH as u64
                        / DEFAULT_TICKS_PER_SLOT,
                ),
                hashes_per_tick: None,
                target_tick_count: None,
            },
            cluster_type: ClusterType::MainnetBeta,

            ..GenesisConfig::default()
        }));

        // enable lazy rent collection because this test depends on rent-due accounts
        // not being eagerly-collected for exact rewards calculation
        bank.restore_old_behavior_for_fragile_tests();

        let sysvar_and_native_proram_delta = 10;
        assert_eq!(
            bank.capitalization(),
            42 * 1_000_000_000 + sysvar_and_native_proram_delta
        );
        assert!(bank.rewards.read().unwrap().is_empty());

        let vote_id = solana_sdk::pubkey::new_rand();
        let mut vote_account =
            vote_state::create_account(&vote_id, &solana_sdk::pubkey::new_rand(), 50, 100);
        let (stake_id1, stake_account1) = crate::stakes::tests::create_stake_account(123, &vote_id);
        let (stake_id2, stake_account2) = crate::stakes::tests::create_stake_account(456, &vote_id);

        // set up accounts
        bank.store_account_and_update_capitalization(&stake_id1, &stake_account1);
        bank.store_account_and_update_capitalization(&stake_id2, &stake_account2);

        // generate some rewards
        let mut vote_state = Some(VoteState::from(&vote_account).unwrap());
        for i in 0..MAX_LOCKOUT_HISTORY + 42 {
            if let Some(v) = vote_state.as_mut() {
                v.process_slot_vote_unchecked(i as u64)
            }
            let versioned = VoteStateVersions::Current(Box::new(vote_state.take().unwrap()));
            VoteState::to(&versioned, &mut vote_account).unwrap();
            bank.store_account_and_update_capitalization(&vote_id, &vote_account);
            match versioned {
                VoteStateVersions::Current(v) => {
                    vote_state = Some(*v);
                }
                _ => panic!("Has to be of type Current"),
            };
        }
        bank.store_account_and_update_capitalization(&vote_id, &vote_account);

        // put a child bank in epoch 1, which calls update_rewards()...
        let bank1 = Bank::new_from_parent(
            &bank,
            &Pubkey::default(),
            bank.get_slots_in_epoch(bank.epoch()) + 1,
        );
        // verify that there's inflation
        assert_ne!(bank1.capitalization(), bank.capitalization());

        bank1.freeze();
        assert!(bank1.calculate_and_verify_capitalization());

        // verify voting and staking rewards are recorded
        let rewards = bank1.rewards.read().unwrap();
        rewards
            .iter()
            .find(|(_address, reward)| reward.reward_type == RewardType::Voting)
            .unwrap();
        rewards
            .iter()
            .find(|(_address, reward)| reward.reward_type == RewardType::Staking)
            .unwrap();

        bank1.capitalization()
    }

    #[test]
    fn test_bank_update_rewards_determinism() {
        solana_logger::setup();

        // The same reward should be distributed given same credits
        let expected_capitalization = do_test_bank_update_rewards_determinism();
        // Repeat somewhat large number of iterations to expose possible different behavior
        // depending on the randomly-seeded HashMap ordering
        for _ in 0..30 {
            let actual_capitalization = do_test_bank_update_rewards_determinism();
            assert_eq!(actual_capitalization, expected_capitalization);
        }
    }

    #[test]
    fn test_bank_tx_fee() {
        solana_logger::setup();

        let arbitrary_transfer_amount = 42;
        let mint = arbitrary_transfer_amount * 100;
        let leader = solana_sdk::pubkey::new_rand();
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_leader(mint, &leader, 3);
        genesis_config.fee_rate_governor = FeeRateGovernor::new(4, 0); // something divisible by 2

        let expected_fee_paid = genesis_config
            .fee_rate_governor
            .create_fee_calculator()
            .lamports_per_signature;
        let (expected_fee_collected, expected_fee_burned) =
            genesis_config.fee_rate_governor.burn(expected_fee_paid);

        let mut bank = Bank::new(&genesis_config);

        let capitalization = bank.capitalization();

        let key = Keypair::new();
        let tx = system_transaction::transfer(
            &mint_keypair,
            &key.pubkey(),
            arbitrary_transfer_amount,
            bank.last_blockhash(),
        );

        let initial_balance = bank.get_balance(&leader);
        assert_eq!(bank.process_transaction(&tx), Ok(()));
        assert_eq!(bank.get_balance(&key.pubkey()), arbitrary_transfer_amount);
        assert_eq!(
            bank.get_balance(&mint_keypair.pubkey()),
            mint - arbitrary_transfer_amount - expected_fee_paid
        );

        assert_eq!(bank.get_balance(&leader), initial_balance);
        goto_end_of_slot(&mut bank);
        assert_eq!(bank.signature_count(), 1);
        assert_eq!(
            bank.get_balance(&leader),
            initial_balance + expected_fee_collected
        ); // Leader collects fee after the bank is frozen

        // verify capitalization
        let sysvar_and_native_proram_delta = 1;
        assert_eq!(
            capitalization - expected_fee_burned + sysvar_and_native_proram_delta,
            bank.capitalization()
        );

        assert_eq!(
            *bank.rewards.read().unwrap(),
            vec![(
                leader,
                RewardInfo {
                    reward_type: RewardType::Fee,
                    lamports: expected_fee_collected as i64,
                    post_balance: initial_balance + expected_fee_collected,
                }
            )]
        );

        // Verify that an InstructionError collects fees, too
        let mut bank = Bank::new_from_parent(&Arc::new(bank), &leader, 1);
        let mut tx =
            system_transaction::transfer(&mint_keypair, &key.pubkey(), 1, bank.last_blockhash());
        // Create a bogus instruction to system_program to cause an instruction error
        tx.message.instructions[0].data[0] = 40;

        bank.process_transaction(&tx)
            .expect_err("instruction error");
        assert_eq!(bank.get_balance(&key.pubkey()), arbitrary_transfer_amount); // no change
        assert_eq!(
            bank.get_balance(&mint_keypair.pubkey()),
            mint - arbitrary_transfer_amount - 2 * expected_fee_paid
        ); // mint_keypair still pays a fee
        goto_end_of_slot(&mut bank);
        assert_eq!(bank.signature_count(), 1);

        // Profit! 2 transaction signatures processed at 3 lamports each
        assert_eq!(
            bank.get_balance(&leader),
            initial_balance + 2 * expected_fee_collected
        );

        assert_eq!(
            *bank.rewards.read().unwrap(),
            vec![(
                leader,
                RewardInfo {
                    reward_type: RewardType::Fee,
                    lamports: expected_fee_collected as i64,
                    post_balance: initial_balance + 2 * expected_fee_collected,
                }
            )]
        );
    }

    #[test]
    fn test_bank_blockhash_fee_schedule() {
        //solana_logger::setup();

        let leader = solana_sdk::pubkey::new_rand();
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_leader(1_000_000, &leader, 3);
        genesis_config
            .fee_rate_governor
            .target_lamports_per_signature = 1000;
        genesis_config.fee_rate_governor.target_signatures_per_slot = 1;

        let mut bank = Bank::new(&genesis_config);
        goto_end_of_slot(&mut bank);
        let (cheap_blockhash, cheap_fee_calculator) = bank.last_blockhash_with_fee_calculator();
        assert_eq!(cheap_fee_calculator.lamports_per_signature, 0);

        let mut bank = Bank::new_from_parent(&Arc::new(bank), &leader, 1);
        goto_end_of_slot(&mut bank);
        let (expensive_blockhash, expensive_fee_calculator) =
            bank.last_blockhash_with_fee_calculator();
        assert!(
            cheap_fee_calculator.lamports_per_signature
                < expensive_fee_calculator.lamports_per_signature
        );

        let bank = Bank::new_from_parent(&Arc::new(bank), &leader, 2);

        // Send a transfer using cheap_blockhash
        let key = Keypair::new();
        let initial_mint_balance = bank.get_balance(&mint_keypair.pubkey());
        let tx = system_transaction::transfer(&mint_keypair, &key.pubkey(), 1, cheap_blockhash);
        assert_eq!(bank.process_transaction(&tx), Ok(()));
        assert_eq!(bank.get_balance(&key.pubkey()), 1);
        assert_eq!(
            bank.get_balance(&mint_keypair.pubkey()),
            initial_mint_balance - 1 - cheap_fee_calculator.lamports_per_signature
        );

        // Send a transfer using expensive_blockhash
        let key = Keypair::new();
        let initial_mint_balance = bank.get_balance(&mint_keypair.pubkey());
        let tx = system_transaction::transfer(&mint_keypair, &key.pubkey(), 1, expensive_blockhash);
        assert_eq!(bank.process_transaction(&tx), Ok(()));
        assert_eq!(bank.get_balance(&key.pubkey()), 1);
        assert_eq!(
            bank.get_balance(&mint_keypair.pubkey()),
            initial_mint_balance - 1 - expensive_fee_calculator.lamports_per_signature
        );
    }

    #[test]
    fn test_debits_before_credits() {
        let (genesis_config, mint_keypair) = create_genesis_config(2);
        let bank = Bank::new(&genesis_config);
        let keypair = Keypair::new();
        let tx0 = system_transaction::transfer(
            &mint_keypair,
            &keypair.pubkey(),
            2,
            genesis_config.hash(),
        );
        let tx1 = system_transaction::transfer(
            &keypair,
            &mint_keypair.pubkey(),
            1,
            genesis_config.hash(),
        );
        let txs = vec![tx0, tx1];
        let results = bank.process_transactions(&txs);
        assert!(results[1].is_err());

        // Assert bad transactions aren't counted.
        assert_eq!(bank.transaction_count(), 1);
    }

    #[test]
    fn test_interleaving_locks() {
        let (genesis_config, mint_keypair) = create_genesis_config(3);
        let bank = Bank::new(&genesis_config);
        let alice = Keypair::new();
        let bob = Keypair::new();

        let tx1 =
            system_transaction::transfer(&mint_keypair, &alice.pubkey(), 1, genesis_config.hash());
        let pay_alice = vec![tx1];

        let lock_result = bank.prepare_batch(pay_alice.iter());
        let results_alice = bank
            .load_execute_and_commit_transactions(
                &lock_result,
                MAX_PROCESSING_AGE,
                false,
                false,
                false,
                &mut ExecuteTimings::default(),
            )
            .0
            .fee_collection_results;
        assert_eq!(results_alice[0], Ok(()));

        // try executing an interleaved transfer twice
        assert_eq!(
            bank.transfer(1, &mint_keypair, &bob.pubkey()),
            Err(TransactionError::AccountInUse)
        );
        // the second time should fail as well
        // this verifies that `unlock_accounts` doesn't unlock `AccountInUse` accounts
        assert_eq!(
            bank.transfer(1, &mint_keypair, &bob.pubkey()),
            Err(TransactionError::AccountInUse)
        );

        drop(lock_result);

        assert!(bank.transfer(2, &mint_keypair, &bob.pubkey()).is_ok());
    }

    #[test]
    fn test_readonly_relaxed_locks() {
        let (genesis_config, _) = create_genesis_config(3);
        let bank = Bank::new(&genesis_config);
        let key0 = Keypair::new();
        let key1 = Keypair::new();
        let key2 = Keypair::new();
        let key3 = solana_sdk::pubkey::new_rand();

        let message = Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![key0.pubkey(), key3],
            recent_blockhash: Hash::default(),
            instructions: vec![],
        };
        let tx = Transaction::new(&[&key0], message, genesis_config.hash());
        let txs = vec![tx];

        let batch0 = bank.prepare_batch(txs.iter());
        assert!(batch0.lock_results()[0].is_ok());

        // Try locking accounts, locking a previously read-only account as writable
        // should fail
        let message = Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![key1.pubkey(), key3],
            recent_blockhash: Hash::default(),
            instructions: vec![],
        };
        let tx = Transaction::new(&[&key1], message, genesis_config.hash());
        let txs = vec![tx];

        let batch1 = bank.prepare_batch(txs.iter());
        assert!(batch1.lock_results()[0].is_err());

        // Try locking a previously read-only account a 2nd time; should succeed
        let message = Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![key2.pubkey(), key3],
            recent_blockhash: Hash::default(),
            instructions: vec![],
        };
        let tx = Transaction::new(&[&key2], message, genesis_config.hash());
        let txs = vec![tx];

        let batch2 = bank.prepare_batch(txs.iter());
        assert!(batch2.lock_results()[0].is_ok());
    }

    #[test]
    fn test_bank_invalid_account_index() {
        let (genesis_config, mint_keypair) = create_genesis_config(1);
        let keypair = Keypair::new();
        let bank = Bank::new(&genesis_config);

        let tx = system_transaction::transfer(
            &mint_keypair,
            &keypair.pubkey(),
            1,
            genesis_config.hash(),
        );

        let mut tx_invalid_program_index = tx.clone();
        tx_invalid_program_index.message.instructions[0].program_id_index = 42;
        assert_eq!(
            bank.process_transaction(&tx_invalid_program_index),
            Err(TransactionError::SanitizeFailure)
        );

        let mut tx_invalid_account_index = tx;
        tx_invalid_account_index.message.instructions[0].accounts[0] = 42;
        assert_eq!(
            bank.process_transaction(&tx_invalid_account_index),
            Err(TransactionError::SanitizeFailure)
        );
    }

    #[test]
    fn test_bank_pay_to_self() {
        let (genesis_config, mint_keypair) = create_genesis_config(1);
        let key1 = Keypair::new();
        let bank = Bank::new(&genesis_config);

        bank.transfer(1, &mint_keypair, &key1.pubkey()).unwrap();
        assert_eq!(bank.get_balance(&key1.pubkey()), 1);
        let tx = system_transaction::transfer(&key1, &key1.pubkey(), 1, genesis_config.hash());
        let _res = bank.process_transaction(&tx);

        assert_eq!(bank.get_balance(&key1.pubkey()), 1);
        bank.get_signature_status(&tx.signatures[0])
            .unwrap()
            .unwrap();
    }

    fn new_from_parent(parent: &Arc<Bank>) -> Bank {
        Bank::new_from_parent(parent, &Pubkey::default(), parent.slot() + 1)
    }

    /// Verify that the parent's vector is computed correctly
    #[test]
    fn test_bank_parents() {
        let (genesis_config, _) = create_genesis_config(1);
        let parent = Arc::new(Bank::new(&genesis_config));

        let bank = new_from_parent(&parent);
        assert!(Arc::ptr_eq(&bank.parents()[0], &parent));
    }

    /// Verifies that transactions are dropped if they have already been processed
    #[test]
    fn test_tx_already_processed() {
        let (genesis_config, mint_keypair) = create_genesis_config(2);
        let mut bank = Bank::new(&genesis_config);
        assert!(!bank.check_duplicates_by_hash_enabled());

        let key1 = Keypair::new();
        let mut tx =
            system_transaction::transfer(&mint_keypair, &key1.pubkey(), 1, genesis_config.hash());

        // First process `tx` so that the status cache is updated
        assert_eq!(bank.process_transaction(&tx), Ok(()));

        // Ensure that signature check works
        assert_eq!(
            bank.process_transaction(&tx),
            Err(TransactionError::AlreadyProcessed)
        );

        // Clear transaction signature
        tx.signatures[0] = Signature::default();

        // Enable duplicate check by message hash
        bank.feature_set = Arc::new(FeatureSet::all_enabled());

        // Ensure that message hash check works
        assert_eq!(
            bank.process_transaction(&tx),
            Err(TransactionError::AlreadyProcessed)
        );
    }

    /// Verifies that last ids and status cache are correctly referenced from parent
    #[test]
    fn test_bank_parent_already_processed() {
        let (genesis_config, mint_keypair) = create_genesis_config(2);
        let key1 = Keypair::new();
        let parent = Arc::new(Bank::new(&genesis_config));

        let tx =
            system_transaction::transfer(&mint_keypair, &key1.pubkey(), 1, genesis_config.hash());
        assert_eq!(parent.process_transaction(&tx), Ok(()));
        let bank = new_from_parent(&parent);
        assert_eq!(
            bank.process_transaction(&tx),
            Err(TransactionError::AlreadyProcessed)
        );
    }

    /// Verifies that last ids and accounts are correctly referenced from parent
    #[test]
    fn test_bank_parent_account_spend() {
        let (genesis_config, mint_keypair) = create_genesis_config(2);
        let key1 = Keypair::new();
        let key2 = Keypair::new();
        let parent = Arc::new(Bank::new(&genesis_config));

        let tx =
            system_transaction::transfer(&mint_keypair, &key1.pubkey(), 1, genesis_config.hash());
        assert_eq!(parent.process_transaction(&tx), Ok(()));
        let bank = new_from_parent(&parent);
        let tx = system_transaction::transfer(&key1, &key2.pubkey(), 1, genesis_config.hash());
        assert_eq!(bank.process_transaction(&tx), Ok(()));
        assert_eq!(parent.get_signature_status(&tx.signatures[0]), None);
    }

    #[test]
    fn test_bank_hash_internal_state() {
        let (genesis_config, mint_keypair) = create_genesis_config(2_000);
        let bank0 = Bank::new(&genesis_config);
        let bank1 = Bank::new(&genesis_config);
        let initial_state = bank0.hash_internal_state();
        assert_eq!(bank1.hash_internal_state(), initial_state);

        let pubkey = solana_sdk::pubkey::new_rand();
        bank0.transfer(1_000, &mint_keypair, &pubkey).unwrap();
        assert_ne!(bank0.hash_internal_state(), initial_state);
        bank1.transfer(1_000, &mint_keypair, &pubkey).unwrap();
        assert_eq!(bank0.hash_internal_state(), bank1.hash_internal_state());

        // Checkpointing should always result in a new state
        let bank2 = new_from_parent(&Arc::new(bank1));
        assert_ne!(bank0.hash_internal_state(), bank2.hash_internal_state());

        let pubkey2 = solana_sdk::pubkey::new_rand();
        info!("transfer 2 {}", pubkey2);
        bank2.transfer(10, &mint_keypair, &pubkey2).unwrap();
        bank2.update_accounts_hash();
        assert!(bank2.verify_bank_hash());
    }

    #[test]
    fn test_bank_hash_internal_state_verify() {
        solana_logger::setup();
        let (genesis_config, mint_keypair) = create_genesis_config(2_000);
        let bank0 = Bank::new(&genesis_config);

        let pubkey = solana_sdk::pubkey::new_rand();
        info!("transfer 0 {} mint: {}", pubkey, mint_keypair.pubkey());
        bank0.transfer(1_000, &mint_keypair, &pubkey).unwrap();

        let bank0_state = bank0.hash_internal_state();
        let bank0 = Arc::new(bank0);
        // Checkpointing should result in a new state while freezing the parent
        let bank2 = Bank::new_from_parent(&bank0, &solana_sdk::pubkey::new_rand(), 1);
        assert_ne!(bank0_state, bank2.hash_internal_state());
        // Checkpointing should modify the checkpoint's state when freezed
        assert_ne!(bank0_state, bank0.hash_internal_state());

        // Checkpointing should never modify the checkpoint's state once frozen
        let bank0_state = bank0.hash_internal_state();
        bank2.update_accounts_hash();
        assert!(bank2.verify_bank_hash());
        let bank3 = Bank::new_from_parent(&bank0, &solana_sdk::pubkey::new_rand(), 2);
        assert_eq!(bank0_state, bank0.hash_internal_state());
        assert!(bank2.verify_bank_hash());
        bank3.update_accounts_hash();
        assert!(bank3.verify_bank_hash());

        let pubkey2 = solana_sdk::pubkey::new_rand();
        info!("transfer 2 {}", pubkey2);
        bank2.transfer(10, &mint_keypair, &pubkey2).unwrap();
        bank2.update_accounts_hash();
        assert!(bank2.verify_bank_hash());
        assert!(bank3.verify_bank_hash());
    }

    #[test]
    #[should_panic(expected = "assertion failed: self.is_frozen()")]
    fn test_verify_hash_unfrozen() {
        let (genesis_config, _mint_keypair) = create_genesis_config(2_000);
        let bank = Bank::new(&genesis_config);
        assert!(bank.verify_hash());
    }

    #[test]
    fn test_verify_snapshot_bank() {
        solana_logger::setup();
        let pubkey = solana_sdk::pubkey::new_rand();
        let (genesis_config, mint_keypair) = create_genesis_config(2_000);
        let bank = Bank::new(&genesis_config);
        bank.transfer(1_000, &mint_keypair, &pubkey).unwrap();
        bank.freeze();
        bank.update_accounts_hash();
        assert!(bank.verify_snapshot_bank());

        // tamper the bank after freeze!
        bank.increment_signature_count(1);
        assert!(!bank.verify_snapshot_bank());
    }

    // Test that two bank forks with the same accounts should not hash to the same value.
    #[test]
    fn test_bank_hash_internal_state_same_account_different_fork() {
        solana_logger::setup();
        let (genesis_config, mint_keypair) = create_genesis_config(2_000);
        let bank0 = Arc::new(Bank::new(&genesis_config));
        let initial_state = bank0.hash_internal_state();
        let bank1 = Bank::new_from_parent(&bank0, &Pubkey::default(), 1);
        assert_ne!(bank1.hash_internal_state(), initial_state);

        info!("transfer bank1");
        let pubkey = solana_sdk::pubkey::new_rand();
        bank1.transfer(1_000, &mint_keypair, &pubkey).unwrap();
        assert_ne!(bank1.hash_internal_state(), initial_state);

        info!("transfer bank2");
        // bank2 should not hash the same as bank1
        let bank2 = Bank::new_from_parent(&bank0, &Pubkey::default(), 2);
        bank2.transfer(1_000, &mint_keypair, &pubkey).unwrap();
        assert_ne!(bank2.hash_internal_state(), initial_state);
        assert_ne!(bank1.hash_internal_state(), bank2.hash_internal_state());
    }

    #[test]
    fn test_hash_internal_state_genesis() {
        let bank0 = Bank::new(&create_genesis_config(10).0);
        let bank1 = Bank::new(&create_genesis_config(20).0);
        assert_ne!(bank0.hash_internal_state(), bank1.hash_internal_state());
    }

    // See that the order of two transfers does not affect the result
    // of hash_internal_state
    #[test]
    fn test_hash_internal_state_order() {
        let (genesis_config, mint_keypair) = create_genesis_config(100);
        let bank0 = Bank::new(&genesis_config);
        let bank1 = Bank::new(&genesis_config);
        assert_eq!(bank0.hash_internal_state(), bank1.hash_internal_state());
        let key0 = solana_sdk::pubkey::new_rand();
        let key1 = solana_sdk::pubkey::new_rand();
        bank0.transfer(10, &mint_keypair, &key0).unwrap();
        bank0.transfer(20, &mint_keypair, &key1).unwrap();

        bank1.transfer(20, &mint_keypair, &key1).unwrap();
        bank1.transfer(10, &mint_keypair, &key0).unwrap();

        assert_eq!(bank0.hash_internal_state(), bank1.hash_internal_state());
    }

    #[test]
    fn test_bank_get_account_modified_since_parent_with_fixed_root() {
        let pubkey = solana_sdk::pubkey::new_rand();

        let (genesis_config, mint_keypair) = create_genesis_config(500);
        let bank1 = Arc::new(Bank::new(&genesis_config));
        bank1.transfer(1, &mint_keypair, &pubkey).unwrap();
        let result = bank1.get_account_modified_since_parent_with_fixed_root(&pubkey);
        assert!(result.is_some());
        let (account, slot) = result.unwrap();
        assert_eq!(account.lamports(), 1);
        assert_eq!(slot, 0);

        let bank2 = Arc::new(Bank::new_from_parent(&bank1, &Pubkey::default(), 1));
        assert!(bank2
            .get_account_modified_since_parent_with_fixed_root(&pubkey)
            .is_none());
        bank2.transfer(100, &mint_keypair, &pubkey).unwrap();
        let result = bank1.get_account_modified_since_parent_with_fixed_root(&pubkey);
        assert!(result.is_some());
        let (account, slot) = result.unwrap();
        assert_eq!(account.lamports(), 1);
        assert_eq!(slot, 0);
        let result = bank2.get_account_modified_since_parent_with_fixed_root(&pubkey);
        assert!(result.is_some());
        let (account, slot) = result.unwrap();
        assert_eq!(account.lamports(), 101);
        assert_eq!(slot, 1);

        bank1.squash();

        let bank3 = Bank::new_from_parent(&bank2, &Pubkey::default(), 3);
        assert_eq!(
            None,
            bank3.get_account_modified_since_parent_with_fixed_root(&pubkey)
        );
    }

    #[test]
    fn test_bank_epoch_vote_accounts() {
        let leader_pubkey = solana_sdk::pubkey::new_rand();
        let leader_lamports = 3;
        let mut genesis_config =
            create_genesis_config_with_leader(5, &leader_pubkey, leader_lamports).genesis_config;

        // set this up weird, forces future generation, odd mod(), etc.
        //  this says: "vote_accounts for epoch X should be generated at slot index 3 in epoch X-2...
        const SLOTS_PER_EPOCH: u64 = MINIMUM_SLOTS_PER_EPOCH as u64;
        const LEADER_SCHEDULE_SLOT_OFFSET: u64 = SLOTS_PER_EPOCH * 3 - 3;
        // no warmup allows me to do the normal division stuff below
        genesis_config.epoch_schedule =
            EpochSchedule::custom(SLOTS_PER_EPOCH, LEADER_SCHEDULE_SLOT_OFFSET, false);

        let parent = Arc::new(Bank::new(&genesis_config));
        let mut leader_vote_stake: Vec<_> = parent
            .epoch_vote_accounts(0)
            .map(|accounts| {
                accounts
                    .iter()
                    .filter_map(|(pubkey, (stake, account))| {
                        if let Ok(vote_state) = account.vote_state().as_ref() {
                            if vote_state.node_pubkey == leader_pubkey {
                                Some((*pubkey, *stake))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap();
        assert_eq!(leader_vote_stake.len(), 1);
        let (leader_vote_account, leader_stake) = leader_vote_stake.pop().unwrap();
        assert!(leader_stake > 0);

        let leader_stake = Stake {
            delegation: Delegation {
                stake: leader_lamports,
                activation_epoch: std::u64::MAX, // bootstrap
                ..Delegation::default()
            },
            ..Stake::default()
        };

        let mut epoch = 1;
        loop {
            if epoch > LEADER_SCHEDULE_SLOT_OFFSET / SLOTS_PER_EPOCH {
                break;
            }
            let vote_accounts = parent.epoch_vote_accounts(epoch);
            assert!(vote_accounts.is_some());

            // epoch_stakes are a snapshot at the leader_schedule_slot_offset boundary
            //   in the prior epoch (0 in this case)
            assert_eq!(
                leader_stake.stake(0, None, true),
                vote_accounts.unwrap().get(&leader_vote_account).unwrap().0
            );

            epoch += 1;
        }

        // child crosses epoch boundary and is the first slot in the epoch
        let child = Bank::new_from_parent(
            &parent,
            &leader_pubkey,
            SLOTS_PER_EPOCH - (LEADER_SCHEDULE_SLOT_OFFSET % SLOTS_PER_EPOCH),
        );

        assert!(child.epoch_vote_accounts(epoch).is_some());
        assert_eq!(
            leader_stake.stake(child.epoch(), None, true),
            child
                .epoch_vote_accounts(epoch)
                .unwrap()
                .get(&leader_vote_account)
                .unwrap()
                .0
        );

        // child crosses epoch boundary but isn't the first slot in the epoch, still
        //  makes an epoch stakes snapshot at 1
        let child = Bank::new_from_parent(
            &parent,
            &leader_pubkey,
            SLOTS_PER_EPOCH - (LEADER_SCHEDULE_SLOT_OFFSET % SLOTS_PER_EPOCH) + 1,
        );
        assert!(child.epoch_vote_accounts(epoch).is_some());
        assert_eq!(
            leader_stake.stake(child.epoch(), None, true),
            child
                .epoch_vote_accounts(epoch)
                .unwrap()
                .get(&leader_vote_account)
                .unwrap()
                .0
        );
    }

    #[test]
    fn test_zero_signatures() {
        solana_logger::setup();
        let (genesis_config, mint_keypair) = create_genesis_config(500);
        let mut bank = Bank::new(&genesis_config);
        bank.fee_calculator.lamports_per_signature = 2;
        let key = Keypair::new();

        let mut transfer_instruction =
            system_instruction::transfer(&mint_keypair.pubkey(), &key.pubkey(), 0);
        transfer_instruction.accounts[0].is_signer = false;
        let message = Message::new(&[transfer_instruction], None);
        let tx = Transaction::new(&[&Keypair::new(); 0], message, bank.last_blockhash());

        assert_eq!(
            bank.process_transaction(&tx),
            Err(TransactionError::SanitizeFailure)
        );
        assert_eq!(bank.get_balance(&key.pubkey()), 0);
    }

    #[test]
    fn test_is_delta_true() {
        let (genesis_config, mint_keypair) = create_genesis_config(500);
        let bank = Arc::new(Bank::new(&genesis_config));
        let key1 = Keypair::new();
        let tx_transfer_mint_to_1 =
            system_transaction::transfer(&mint_keypair, &key1.pubkey(), 1, genesis_config.hash());
        assert_eq!(bank.process_transaction(&tx_transfer_mint_to_1), Ok(()));
        assert_eq!(bank.is_delta.load(Relaxed), true);

        let bank1 = new_from_parent(&bank);
        let hash1 = bank1.hash_internal_state();
        assert_eq!(bank1.is_delta.load(Relaxed), false);
        assert_ne!(hash1, bank.hash());
        // ticks don't make a bank into a delta or change its state unless a block boundary is crossed
        bank1.register_tick(&Hash::default());
        assert_eq!(bank1.is_delta.load(Relaxed), false);
        assert_eq!(bank1.hash_internal_state(), hash1);
    }

    #[test]
    fn test_is_empty() {
        let (genesis_config, mint_keypair) = create_genesis_config(500);
        let bank0 = Arc::new(Bank::new(&genesis_config));
        let key1 = Keypair::new();

        // The zeroth bank is empty becasue there are no transactions
        assert_eq!(bank0.is_empty(), true);

        // Set is_delta to true, bank is no longer empty
        let tx_transfer_mint_to_1 =
            system_transaction::transfer(&mint_keypair, &key1.pubkey(), 1, genesis_config.hash());
        assert_eq!(bank0.process_transaction(&tx_transfer_mint_to_1), Ok(()));
        assert_eq!(bank0.is_empty(), false);
    }

    #[test]
    fn test_bank_inherit_tx_count() {
        let (genesis_config, mint_keypair) = create_genesis_config(500);
        let bank0 = Arc::new(Bank::new(&genesis_config));

        // Bank 1
        let bank1 = Arc::new(Bank::new_from_parent(
            &bank0,
            &solana_sdk::pubkey::new_rand(),
            1,
        ));
        // Bank 2
        let bank2 = Bank::new_from_parent(&bank0, &solana_sdk::pubkey::new_rand(), 2);

        // transfer a token
        assert_eq!(
            bank1.process_transaction(&system_transaction::transfer(
                &mint_keypair,
                &Keypair::new().pubkey(),
                1,
                genesis_config.hash(),
            )),
            Ok(())
        );

        assert_eq!(bank0.transaction_count(), 0);
        assert_eq!(bank2.transaction_count(), 0);
        assert_eq!(bank1.transaction_count(), 1);

        bank1.squash();

        assert_eq!(bank0.transaction_count(), 0);
        assert_eq!(bank2.transaction_count(), 0);
        assert_eq!(bank1.transaction_count(), 1);

        let bank6 = Bank::new_from_parent(&bank1, &solana_sdk::pubkey::new_rand(), 3);
        assert_eq!(bank1.transaction_count(), 1);
        assert_eq!(bank6.transaction_count(), 1);

        bank6.squash();
        assert_eq!(bank6.transaction_count(), 1);
    }

    #[test]
    fn test_bank_inherit_fee_rate_governor() {
        let (mut genesis_config, _mint_keypair) = create_genesis_config(500);
        genesis_config
            .fee_rate_governor
            .target_lamports_per_signature = 123;

        let bank0 = Arc::new(Bank::new(&genesis_config));
        let bank1 = Arc::new(new_from_parent(&bank0));
        assert_eq!(
            bank0.fee_rate_governor.target_lamports_per_signature / 2,
            bank1
                .fee_rate_governor
                .create_fee_calculator()
                .lamports_per_signature
        );
    }

    #[test]
    fn test_bank_vote_accounts() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_leader(500, &solana_sdk::pubkey::new_rand(), 1);
        let bank = Arc::new(Bank::new(&genesis_config));

        let vote_accounts = bank.vote_accounts();
        assert_eq!(vote_accounts.len(), 1); // bootstrap validator has
                                            // to have a vote account

        let vote_keypair = Keypair::new();
        let instructions = vote_instruction::create_account(
            &mint_keypair.pubkey(),
            &vote_keypair.pubkey(),
            &VoteInit {
                node_pubkey: mint_keypair.pubkey(),
                authorized_voter: vote_keypair.pubkey(),
                authorized_withdrawer: vote_keypair.pubkey(),
                commission: 0,
            },
            10,
        );

        let message = Message::new(&instructions, Some(&mint_keypair.pubkey()));
        let transaction = Transaction::new(
            &[&mint_keypair, &vote_keypair],
            message,
            bank.last_blockhash(),
        );

        bank.process_transaction(&transaction).unwrap();

        let vote_accounts = bank.vote_accounts().into_iter().collect::<HashMap<_, _>>();

        assert_eq!(vote_accounts.len(), 2);

        assert!(vote_accounts.get(&vote_keypair.pubkey()).is_some());

        assert!(bank.withdraw(&vote_keypair.pubkey(), 10).is_ok());

        let vote_accounts = bank.vote_accounts();

        assert_eq!(vote_accounts.len(), 1);
    }

    #[test]
    fn test_bank_cloned_stake_delegations() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_leader(500, &solana_sdk::pubkey::new_rand(), 1);
        let bank = Arc::new(Bank::new(&genesis_config));

        let stake_delegations = bank.cloned_stake_delegations();
        assert_eq!(stake_delegations.len(), 1); // bootstrap validator has
                                                // to have a stake delegation

        let vote_keypair = Keypair::new();
        let mut instructions = vote_instruction::create_account(
            &mint_keypair.pubkey(),
            &vote_keypair.pubkey(),
            &VoteInit {
                node_pubkey: mint_keypair.pubkey(),
                authorized_voter: vote_keypair.pubkey(),
                authorized_withdrawer: vote_keypair.pubkey(),
                commission: 0,
            },
            10,
        );

        let stake_keypair = Keypair::new();
        instructions.extend(stake_instruction::create_account_and_delegate_stake(
            &mint_keypair.pubkey(),
            &stake_keypair.pubkey(),
            &vote_keypair.pubkey(),
            &Authorized::auto(&stake_keypair.pubkey()),
            &Lockup::default(),
            10,
        ));

        let message = Message::new(&instructions, Some(&mint_keypair.pubkey()));
        let transaction = Transaction::new(
            &[&mint_keypair, &vote_keypair, &stake_keypair],
            message,
            bank.last_blockhash(),
        );

        bank.process_transaction(&transaction).unwrap();

        let stake_delegations = bank.cloned_stake_delegations();
        assert_eq!(stake_delegations.len(), 2);
        assert!(stake_delegations.get(&stake_keypair.pubkey()).is_some());
    }

    #[test]
    fn test_bank_fees_account() {
        let (mut genesis_config, _) = create_genesis_config(500);
        genesis_config.fee_rate_governor = FeeRateGovernor::new(12345, 0);
        let bank = Arc::new(Bank::new(&genesis_config));

        let fees_account = bank.get_account(&sysvar::fees::id()).unwrap();
        let fees = from_account::<Fees, _>(&fees_account).unwrap();
        assert_eq!(
            bank.fee_calculator.lamports_per_signature,
            fees.fee_calculator.lamports_per_signature
        );
        assert_eq!(fees.fee_calculator.lamports_per_signature, 12345);
    }

    #[test]
    fn test_is_delta_with_no_committables() {
        let (genesis_config, mint_keypair) = create_genesis_config(8000);
        let bank = Bank::new(&genesis_config);
        bank.is_delta.store(false, Relaxed);

        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let fail_tx =
            system_transaction::transfer(&keypair1, &keypair2.pubkey(), 1, bank.last_blockhash());

        // Should fail with TransactionError::AccountNotFound, which means
        // the account which this tx operated on will not be committed. Thus
        // the bank is_delta should still be false
        assert_eq!(
            bank.process_transaction(&fail_tx),
            Err(TransactionError::AccountNotFound)
        );

        // Check the bank is_delta is still false
        assert!(!bank.is_delta.load(Relaxed));

        // Should fail with InstructionError, but InstructionErrors are committable,
        // so is_delta should be true
        assert_eq!(
            bank.transfer(10_001, &mint_keypair, &solana_sdk::pubkey::new_rand()),
            Err(TransactionError::InstructionError(
                0,
                SystemError::ResultWithNegativeLamports.into(),
            ))
        );

        assert!(bank.is_delta.load(Relaxed));
    }

    #[test]
    fn test_status_cache_ancestors() {
        solana_logger::setup();
        let (genesis_config, _mint_keypair) = create_genesis_config(500);
        let parent = Arc::new(Bank::new(&genesis_config));
        let bank1 = Arc::new(new_from_parent(&parent));
        let mut bank = bank1;
        for _ in 0..MAX_CACHE_ENTRIES * 2 {
            bank = Arc::new(new_from_parent(&bank));
            bank.squash();
        }

        let bank = new_from_parent(&bank);
        assert_eq!(
            bank.status_cache_ancestors(),
            (bank.slot() - MAX_CACHE_ENTRIES as u64..=bank.slot()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_add_duplicate_static_program() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_leader(500, &solana_sdk::pubkey::new_rand(), 0);
        let mut bank = Bank::new(&genesis_config);

        fn mock_vote_processor(
            _pubkey: &Pubkey,
            _data: &[u8],
            _invoke_context: &mut dyn InvokeContext,
        ) -> std::result::Result<(), InstructionError> {
            Err(InstructionError::Custom(42))
        }

        let mock_account = Keypair::new();
        let mock_validator_identity = Keypair::new();
        let instructions = vote_instruction::create_account(
            &mint_keypair.pubkey(),
            &mock_account.pubkey(),
            &VoteInit {
                node_pubkey: mock_validator_identity.pubkey(),
                ..VoteInit::default()
            },
            1,
        );

        let message = Message::new(&instructions, Some(&mint_keypair.pubkey()));
        let transaction = Transaction::new(
            &[&mint_keypair, &mock_account, &mock_validator_identity],
            message,
            bank.last_blockhash(),
        );

        let vote_loader_account = bank.get_account(&solana_vote_program::id()).unwrap();
        bank.add_builtin(
            "solana_vote_program",
            solana_vote_program::id(),
            mock_vote_processor,
        );
        let new_vote_loader_account = bank.get_account(&solana_vote_program::id()).unwrap();
        // Vote loader account should not be updated since it was included in the genesis config.
        assert_eq!(vote_loader_account.data(), new_vote_loader_account.data());
        assert_eq!(
            bank.process_transaction(&transaction),
            Err(TransactionError::InstructionError(
                1,
                InstructionError::Custom(42)
            ))
        );
    }

    #[test]
    fn test_add_instruction_processor_for_existing_unrelated_accounts() {
        let (genesis_config, _mint_keypair) = create_genesis_config(500);
        let mut bank = Bank::new(&genesis_config);

        fn mock_ix_processor(
            _pubkey: &Pubkey,
            _data: &[u8],
            _invoke_context: &mut dyn InvokeContext,
        ) -> std::result::Result<(), InstructionError> {
            Err(InstructionError::Custom(42))
        }

        // Non-native loader accounts can not be used for instruction processing
        assert!(bank.stakes.read().unwrap().vote_accounts().is_empty());
        assert!(bank.stakes.read().unwrap().stake_delegations().is_empty());
        assert_eq!(bank.calculate_capitalization(), bank.capitalization());

        let ((vote_id, vote_account), (stake_id, stake_account)) =
            crate::stakes::tests::create_staked_node_accounts(1_0000);
        bank.capitalization
            .fetch_add(vote_account.lamports() + stake_account.lamports(), Relaxed);
        bank.store_account(&vote_id, &vote_account);
        bank.store_account(&stake_id, &stake_account);
        assert!(!bank.stakes.read().unwrap().vote_accounts().is_empty());
        assert!(!bank.stakes.read().unwrap().stake_delegations().is_empty());
        assert_eq!(bank.calculate_capitalization(), bank.capitalization());

        bank.add_builtin("mock_program1", vote_id, mock_ix_processor);
        bank.add_builtin("mock_program2", stake_id, mock_ix_processor);
        assert!(bank.stakes.read().unwrap().vote_accounts().is_empty());
        assert!(bank.stakes.read().unwrap().stake_delegations().is_empty());
        assert_eq!(bank.calculate_capitalization(), bank.capitalization());
        assert_eq!(
            "mock_program1",
            String::from_utf8_lossy(&bank.get_account(&vote_id).unwrap_or_default().data())
        );
        assert_eq!(
            "mock_program2",
            String::from_utf8_lossy(&bank.get_account(&stake_id).unwrap_or_default().data())
        );

        // Re-adding builtin programs should be no-op
        bank.update_accounts_hash();
        let old_hash = bank.get_accounts_hash();
        bank.add_builtin("mock_program1", vote_id, mock_ix_processor);
        bank.add_builtin("mock_program2", stake_id, mock_ix_processor);
        bank.update_accounts_hash();
        let new_hash = bank.get_accounts_hash();
        assert_eq!(old_hash, new_hash);
        assert!(bank.stakes.read().unwrap().vote_accounts().is_empty());
        assert!(bank.stakes.read().unwrap().stake_delegations().is_empty());
        assert_eq!(bank.calculate_capitalization(), bank.capitalization());
        assert_eq!(
            "mock_program1",
            String::from_utf8_lossy(&bank.get_account(&vote_id).unwrap_or_default().data())
        );
        assert_eq!(
            "mock_program2",
            String::from_utf8_lossy(&bank.get_account(&stake_id).unwrap_or_default().data())
        );
    }

    #[test]
    fn test_recent_blockhashes_sysvar() {
        let (genesis_config, _mint_keypair) = create_genesis_config(500);
        let mut bank = Arc::new(Bank::new(&genesis_config));
        for i in 1..5 {
            let bhq_account = bank.get_account(&sysvar::recent_blockhashes::id()).unwrap();
            let recent_blockhashes =
                from_account::<sysvar::recent_blockhashes::RecentBlockhashes, _>(&bhq_account)
                    .unwrap();
            // Check length
            assert_eq!(recent_blockhashes.len(), i);
            let most_recent_hash = recent_blockhashes.iter().next().unwrap().blockhash;
            // Check order
            assert_eq!(Some(true), bank.check_hash_age(&most_recent_hash, 0));
            goto_end_of_slot(Arc::get_mut(&mut bank).unwrap());
            bank = Arc::new(new_from_parent(&bank));
        }
    }

    #[test]
    fn test_blockhash_queue_sysvar_consistency() {
        let (genesis_config, _mint_keypair) = create_genesis_config(100_000);
        let mut bank = Arc::new(Bank::new(&genesis_config));
        goto_end_of_slot(Arc::get_mut(&mut bank).unwrap());

        let bhq_account = bank.get_account(&sysvar::recent_blockhashes::id()).unwrap();
        let recent_blockhashes =
            from_account::<sysvar::recent_blockhashes::RecentBlockhashes, _>(&bhq_account).unwrap();

        let sysvar_recent_blockhash = recent_blockhashes[0].blockhash;
        let bank_last_blockhash = bank.last_blockhash();
        assert_eq!(sysvar_recent_blockhash, bank_last_blockhash);
    }

    #[test]
    fn test_bank_inherit_last_vote_sync() {
        let (genesis_config, _) = create_genesis_config(500);
        let bank0 = Arc::new(Bank::new(&genesis_config));
        let last_ts = bank0.last_vote_sync.load(Relaxed);
        assert_eq!(last_ts, 0);
        bank0.last_vote_sync.store(1, Relaxed);
        let bank1 =
            Bank::new_from_parent(&bank0, &Pubkey::default(), bank0.get_slots_in_epoch(0) - 1);
        let last_ts = bank1.last_vote_sync.load(Relaxed);
        assert_eq!(last_ts, 1);
    }

    #[test]
    fn test_hash_internal_state_unchanged() {
        let (genesis_config, _) = create_genesis_config(500);
        let bank0 = Arc::new(Bank::new(&genesis_config));
        bank0.freeze();
        let bank0_hash = bank0.hash();
        let bank1 = Bank::new_from_parent(&bank0, &Pubkey::default(), 1);
        bank1.freeze();
        let bank1_hash = bank1.hash();
        // Checkpointing should always result in a new state
        assert_ne!(bank0_hash, bank1_hash);
    }

    #[test]
    fn test_ticks_change_state() {
        let (genesis_config, _) = create_genesis_config(500);
        let bank = Arc::new(Bank::new(&genesis_config));
        let bank1 = new_from_parent(&bank);
        let hash1 = bank1.hash_internal_state();
        // ticks don't change its state unless a block boundary is crossed
        for _ in 0..genesis_config.ticks_per_slot {
            assert_eq!(bank1.hash_internal_state(), hash1);
            bank1.register_tick(&Hash::default());
        }
        assert_ne!(bank1.hash_internal_state(), hash1);
    }

    #[ignore]
    #[test]
    fn test_banks_leak() {
        fn add_lotsa_stake_accounts(genesis_config: &mut GenesisConfig) {
            const LOTSA: usize = 4_096;

            (0..LOTSA).for_each(|_| {
                let pubkey = solana_sdk::pubkey::new_rand();
                genesis_config.add_account(
                    pubkey,
                    solana_stake_program::stake_state::create_lockup_stake_account(
                        &Authorized::auto(&pubkey),
                        &Lockup::default(),
                        &Rent::default(),
                        50_000_000,
                    ),
                );
            });
        }
        solana_logger::setup();
        let (mut genesis_config, _) = create_genesis_config(100_000_000_000_000);
        add_lotsa_stake_accounts(&mut genesis_config);
        let mut bank = std::sync::Arc::new(Bank::new(&genesis_config));
        let mut num_banks = 0;
        let pid = std::process::id();
        #[cfg(not(target_os = "linux"))]
        error!(
            "\nYou can run this to watch RAM:\n   while read -p 'banks: '; do echo $(( $(ps -o vsize= -p {})/$REPLY));done", pid
        );
        loop {
            num_banks += 1;
            bank = std::sync::Arc::new(new_from_parent(&bank));
            if num_banks % 100 == 0 {
                #[cfg(target_os = "linux")]
                {
                    let pages_consumed = std::fs::read_to_string(format!("/proc/{}/statm", pid))
                        .unwrap()
                        .split_whitespace()
                        .next()
                        .unwrap()
                        .parse::<usize>()
                        .unwrap();
                    error!(
                        "at {} banks: {} mem or {}kB/bank",
                        num_banks,
                        pages_consumed * 4096,
                        (pages_consumed * 4) / num_banks
                    );
                }
                #[cfg(not(target_os = "linux"))]
                {
                    error!("{} banks, sleeping for 5 sec", num_banks);
                    std::thread::sleep(Duration::new(5, 0));
                }
            }
        }
    }

    #[test]
    fn test_incinerator() {
        let (genesis_config, mint_keypair) = create_genesis_config(1_000_000_000_000);
        let bank0 = Arc::new(Bank::new(&genesis_config));

        // Move to the first normal slot so normal rent behaviour applies
        let bank = Bank::new_from_parent(
            &bank0,
            &Pubkey::default(),
            genesis_config.epoch_schedule.first_normal_slot,
        );
        let pre_capitalization = bank.capitalization();

        // Burn a non-rent exempt amount
        let burn_amount = bank.get_minimum_balance_for_rent_exemption(0) - 1;

        assert_eq!(bank.get_balance(&incinerator::id()), 0);
        bank.transfer(burn_amount, &mint_keypair, &incinerator::id())
            .unwrap();
        assert_eq!(bank.get_balance(&incinerator::id()), burn_amount);
        bank.freeze();
        assert_eq!(bank.get_balance(&incinerator::id()), 0);

        // Ensure that no rent was collected, and the entire burn amount was removed from bank
        // capitalization
        assert_eq!(bank.capitalization(), pre_capitalization - burn_amount);
    }

    #[test]
    fn test_program_id_as_payer() {
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

        info!(
            "mint: {} account keys: {:?}",
            mint_keypair.pubkey(),
            tx.message.account_keys
        );
        assert_eq!(tx.message.account_keys.len(), 4);
        tx.message.account_keys.clear();
        tx.message.account_keys.push(solana_vote_program::id());
        tx.message.account_keys.push(mint_keypair.pubkey());
        tx.message.account_keys.push(from_pubkey);
        tx.message.account_keys.push(to_pubkey);
        tx.message.instructions[0].program_id_index = 0;
        tx.message.instructions[0].accounts.clear();
        tx.message.instructions[0].accounts.push(2);
        tx.message.instructions[0].accounts.push(3);

        let result = bank.process_transaction(&tx);
        assert_eq!(result, Err(TransactionError::SanitizeFailure));
    }

    #[allow(clippy::unnecessary_wraps)]
    fn mock_ok_vote_processor(
        _pubkey: &Pubkey,
        _data: &[u8],
        _invoke_context: &mut dyn InvokeContext,
    ) -> std::result::Result<(), InstructionError> {
        Ok(())
    }

    #[test]
    fn test_ref_account_key_after_program_id() {
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

        tx.message.account_keys.push(solana_sdk::pubkey::new_rand());
        assert_eq!(tx.message.account_keys.len(), 5);
        tx.message.instructions[0].accounts.remove(0);
        tx.message.instructions[0].accounts.push(4);

        let result = bank.process_transaction(&tx);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_fuzz_instructions() {
        solana_logger::setup();
        use rand::{thread_rng, Rng};
        let (genesis_config, _mint_keypair) = create_genesis_config(1_000_000_000);
        let mut bank = Bank::new(&genesis_config);

        let max_programs = 5;
        let program_keys: Vec<_> = (0..max_programs)
            .enumerate()
            .map(|i| {
                let key = solana_sdk::pubkey::new_rand();
                let name = format!("program{:?}", i);
                bank.add_builtin(&name, key, mock_ok_vote_processor);
                (key, name.as_bytes().to_vec())
            })
            .collect();
        let max_keys = 100;
        let keys: Vec<_> = (0..max_keys)
            .enumerate()
            .map(|_| {
                let key = solana_sdk::pubkey::new_rand();
                let balance = if thread_rng().gen_ratio(9, 10) {
                    let lamports = if thread_rng().gen_ratio(1, 5) {
                        thread_rng().gen_range(0, 10)
                    } else {
                        thread_rng().gen_range(20, 100)
                    };
                    let space = thread_rng().gen_range(0, 10);
                    let owner = Pubkey::default();
                    let account = AccountSharedData::new(lamports, space, &owner);
                    bank.store_account(&key, &account);
                    lamports
                } else {
                    0
                };
                (key, balance)
            })
            .collect();
        let mut results = HashMap::new();
        for _ in 0..2_000 {
            let num_keys = if thread_rng().gen_ratio(1, 5) {
                thread_rng().gen_range(0, max_keys)
            } else {
                thread_rng().gen_range(1, 4)
            };
            let num_instructions = thread_rng().gen_range(0, max_keys - num_keys);

            let mut account_keys: Vec<_> = if thread_rng().gen_ratio(1, 5) {
                (0..num_keys)
                    .map(|_| {
                        let idx = thread_rng().gen_range(0, keys.len());
                        keys[idx].0
                    })
                    .collect()
            } else {
                let mut inserted = HashSet::new();
                (0..num_keys)
                    .map(|_| {
                        let mut idx;
                        loop {
                            idx = thread_rng().gen_range(0, keys.len());
                            if !inserted.contains(&idx) {
                                break;
                            }
                        }
                        inserted.insert(idx);
                        keys[idx].0
                    })
                    .collect()
            };

            let instructions: Vec<_> = if num_keys > 0 {
                (0..num_instructions)
                    .map(|_| {
                        let num_accounts_to_pass = thread_rng().gen_range(0, num_keys);
                        let account_indexes = (0..num_accounts_to_pass)
                            .map(|_| thread_rng().gen_range(0, num_keys))
                            .collect();
                        let program_index: u8 = thread_rng().gen_range(0, num_keys) as u8;
                        if thread_rng().gen_ratio(4, 5) {
                            let programs_index = thread_rng().gen_range(0, program_keys.len());
                            account_keys[program_index as usize] = program_keys[programs_index].0;
                        }
                        CompiledInstruction::new(program_index, &10, account_indexes)
                    })
                    .collect()
            } else {
                vec![]
            };

            let account_keys_len = std::cmp::max(account_keys.len(), 2);
            let num_signatures = if thread_rng().gen_ratio(1, 5) {
                thread_rng().gen_range(0, account_keys_len + 10)
            } else {
                thread_rng().gen_range(1, account_keys_len)
            };

            let num_required_signatures = if thread_rng().gen_ratio(1, 5) {
                thread_rng().gen_range(0, account_keys_len + 10) as u8
            } else {
                thread_rng().gen_range(1, std::cmp::max(2, num_signatures)) as u8
            };
            let num_readonly_signed_accounts = if thread_rng().gen_ratio(1, 5) {
                thread_rng().gen_range(0, account_keys_len) as u8
            } else {
                let max = if num_required_signatures > 1 {
                    num_required_signatures - 1
                } else {
                    1
                };
                thread_rng().gen_range(0, max) as u8
            };

            let num_readonly_unsigned_accounts = if thread_rng().gen_ratio(1, 5)
                || (num_required_signatures as usize) >= account_keys_len
            {
                thread_rng().gen_range(0, account_keys_len) as u8
            } else {
                thread_rng().gen_range(0, account_keys_len - num_required_signatures as usize) as u8
            };

            let header = MessageHeader {
                num_required_signatures,
                num_readonly_signed_accounts,
                num_readonly_unsigned_accounts,
            };
            let message = Message {
                header,
                account_keys,
                recent_blockhash: bank.last_blockhash(),
                instructions,
            };

            let tx = Transaction {
                signatures: vec![Signature::default(); num_signatures],
                message,
            };

            let result = bank.process_transaction(&tx);
            for (key, balance) in &keys {
                assert_eq!(bank.get_balance(key), *balance);
            }
            for (key, name) in &program_keys {
                let account = bank.get_account(key).unwrap();
                assert!(account.executable());
                assert_eq!(account.data(), name);
            }
            info!("result: {:?}", result);
            let result_key = format!("{:?}", result);
            *results.entry(result_key).or_insert(0) += 1;
        }
        info!("results: {:?}", results);
    }

    #[test]
    fn test_bank_hash_consistency() {
        solana_logger::setup();

        let mut genesis_config = GenesisConfig::new(
            &[(
                Pubkey::new(&[42; 32]),
                AccountSharedData::new(1_000_000_000_000, 0, &system_program::id()),
            )],
            &[],
        );
        genesis_config.creation_time = 0;
        genesis_config.cluster_type = ClusterType::MainnetBeta;
        genesis_config.rent.burn_percent = 100;
        let mut bank = Arc::new(Bank::new(&genesis_config));
        // Check a few slots, cross an epoch boundary
        assert_eq!(bank.get_slots_in_epoch(0), 32);
        loop {
            goto_end_of_slot(Arc::get_mut(&mut bank).unwrap());
            if bank.slot == 0 {
                assert_eq!(
                    bank.hash().to_string(),
                    "Cn7Wmi7w1n9NbK7RGnTQ4LpbJ2LtoJoc1sufiTwb57Ya"
                );
            }
            if bank.slot == 32 {
                assert_eq!(
                    bank.hash().to_string(),
                    "BXupB8XsZukMTnDbKshJ8qPCydWnc8BKtSj7YTJ6gAH"
                );
            }
            if bank.slot == 64 {
                assert_eq!(
                    bank.hash().to_string(),
                    "EDkKefgSMSV1NhxnGnJP7R5AGZ2JZD6oxnoZtGuEGBCU"
                );
            }
            if bank.slot == 128 {
                assert_eq!(
                    bank.hash().to_string(),
                    "AtWu4tubU9zGFChfHtQghQx3RVWtMQu6Rj49rQymFc4z"
                );
                break;
            }
            bank = Arc::new(new_from_parent(&bank));
        }
    }

    #[test]
    fn test_same_program_id_uses_unqiue_executable_accounts() {
        fn nested_processor(
            _program_id: &Pubkey,
            _data: &[u8],
            invoke_context: &mut dyn InvokeContext,
        ) -> result::Result<(), InstructionError> {
            let keyed_accounts = invoke_context.get_keyed_accounts()?;
            assert_eq!(42, keyed_accounts[0].lamports().unwrap());
            let mut account = keyed_accounts[0].try_account_ref_mut()?;
            account.checked_add_lamports(1)?;
            Ok(())
        }

        let (genesis_config, mint_keypair) = create_genesis_config(50000);
        let mut bank = Bank::new(&genesis_config);

        // Add a new program
        let program1_pubkey = solana_sdk::pubkey::new_rand();
        bank.add_builtin("program", program1_pubkey, nested_processor);

        // Add a new program owned by the first
        let program2_pubkey = solana_sdk::pubkey::new_rand();
        let mut program2_account = AccountSharedData::new(42, 1, &program1_pubkey);
        program2_account.set_executable(true);
        bank.store_account(&program2_pubkey, &program2_account);

        let instruction = Instruction::new_with_bincode(program2_pubkey, &10, vec![]);
        let tx = Transaction::new_signed_with_payer(
            &[instruction.clone(), instruction],
            Some(&mint_keypair.pubkey()),
            &[&mint_keypair],
            bank.last_blockhash(),
        );
        assert!(bank.process_transaction(&tx).is_ok());
        assert_eq!(1, bank.get_balance(&program1_pubkey));
        assert_eq!(42, bank.get_balance(&program2_pubkey));
    }

    fn get_shrink_account_size() -> usize {
        let (genesis_config, _mint_keypair) = create_genesis_config(1_000_000_000);

        // Set root for bank 0, with caching disabled so we can get the size
        // of the storage for this slot
        let mut bank0 = Arc::new(Bank::new_with_config(
            &genesis_config,
            HashSet::new(),
            false,
        ));
        bank0.restore_old_behavior_for_fragile_tests();
        goto_end_of_slot(Arc::<Bank>::get_mut(&mut bank0).unwrap());
        bank0.freeze();
        bank0.squash();

        let sizes = bank0
            .rc
            .accounts
            .scan_slot(0, |stored_account| Some(stored_account.stored_size()));

        // Create an account such that it takes SHRINK_RATIO of the total account space for
        // the slot, so when it gets pruned, the storage entry will become a shrink candidate.
        let bank0_total_size: usize = sizes.into_iter().sum();
        let pubkey0_size = (bank0_total_size as f64 / (1.0 - SHRINK_RATIO)).ceil();
        assert!(pubkey0_size / (pubkey0_size + bank0_total_size as f64) > SHRINK_RATIO);
        pubkey0_size as usize
    }

    #[test]
    fn test_shrink_candidate_slots_cached() {
        solana_logger::setup();

        let (genesis_config, _mint_keypair) = create_genesis_config(1_000_000_000);
        let pubkey0 = solana_sdk::pubkey::new_rand();
        let pubkey1 = solana_sdk::pubkey::new_rand();
        let pubkey2 = solana_sdk::pubkey::new_rand();

        // Set root for bank 0, with caching enabled
        let mut bank0 = Arc::new(Bank::new_with_config(&genesis_config, HashSet::new(), true));
        bank0.restore_old_behavior_for_fragile_tests();

        let pubkey0_size = get_shrink_account_size();

        let account0 = AccountSharedData::new(1000, pubkey0_size as usize, &Pubkey::new_unique());
        bank0.store_account(&pubkey0, &account0);

        goto_end_of_slot(Arc::<Bank>::get_mut(&mut bank0).unwrap());
        bank0.freeze();
        bank0.squash();
        // Flush now so that accounts cache cleaning doesn't clean up bank 0 when later
        // slots add updates to the cache
        bank0.force_flush_accounts_cache();

        // Store some lamports in bank 1
        let some_lamports = 123;
        let mut bank1 = Arc::new(new_from_parent(&bank0));
        bank1.deposit(&pubkey1, some_lamports).unwrap();
        bank1.deposit(&pubkey2, some_lamports).unwrap();
        goto_end_of_slot(Arc::<Bank>::get_mut(&mut bank1).unwrap());
        bank1.freeze();
        bank1.squash();
        // Flush now so that accounts cache cleaning doesn't clean up bank 0 when later
        // slots add updates to the cache
        bank1.force_flush_accounts_cache();

        // Store some lamports for pubkey1 in bank 2, root bank 2
        let mut bank2 = Arc::new(new_from_parent(&bank1));
        bank2.deposit(&pubkey1, some_lamports).unwrap();
        bank2.store_account(&pubkey0, &account0);
        goto_end_of_slot(Arc::<Bank>::get_mut(&mut bank2).unwrap());
        bank2.freeze();
        bank2.squash();
        bank2.force_flush_accounts_cache();

        // Clean accounts, which should add earlier slots to the shrink
        // candidate set
        bank2.clean_accounts(false);

        // Slots 0 and 1 should be candidates for shrinking, but slot 2
        // shouldn't because none of its accounts are outdated by a later
        // root
        assert_eq!(bank2.shrink_candidate_slots(), 2);
        let alive_counts: Vec<usize> = (0..3)
            .map(|slot| {
                bank2
                    .rc
                    .accounts
                    .accounts_db
                    .alive_account_count_in_slot(slot)
            })
            .collect();

        // No more slots should be shrunk
        assert_eq!(bank2.shrink_candidate_slots(), 0);
        // alive_counts represents the count of alive accounts in the three slots 0,1,2
        assert_eq!(alive_counts, vec![9, 1, 7]);
    }

    #[test]
    fn test_process_stale_slot_with_budget() {
        solana_logger::setup();

        let (genesis_config, _mint_keypair) = create_genesis_config(1_000_000_000);
        let pubkey1 = solana_sdk::pubkey::new_rand();
        let pubkey2 = solana_sdk::pubkey::new_rand();

        let mut bank = Arc::new(Bank::new(&genesis_config));
        bank.restore_old_behavior_for_fragile_tests();
        assert_eq!(bank.process_stale_slot_with_budget(0, 0), 0);
        assert_eq!(bank.process_stale_slot_with_budget(133, 0), 133);

        assert_eq!(bank.process_stale_slot_with_budget(0, 100), 0);
        assert_eq!(bank.process_stale_slot_with_budget(33, 100), 0);
        assert_eq!(bank.process_stale_slot_with_budget(133, 100), 33);

        goto_end_of_slot(Arc::<Bank>::get_mut(&mut bank).unwrap());

        bank.squash();

        let some_lamports = 123;
        let mut bank = Arc::new(new_from_parent(&bank));
        bank.deposit(&pubkey1, some_lamports).unwrap();
        bank.deposit(&pubkey2, some_lamports).unwrap();

        goto_end_of_slot(Arc::<Bank>::get_mut(&mut bank).unwrap());

        let mut bank = Arc::new(new_from_parent(&bank));
        bank.deposit(&pubkey1, some_lamports).unwrap();

        goto_end_of_slot(Arc::<Bank>::get_mut(&mut bank).unwrap());

        bank.squash();
        bank.clean_accounts(false);
        let force_to_return_alive_account = 0;
        assert_eq!(
            bank.process_stale_slot_with_budget(22, force_to_return_alive_account),
            22
        );

        let consumed_budgets: usize = (0..3)
            .map(|_| bank.process_stale_slot_with_budget(0, force_to_return_alive_account))
            .sum();
        // consumed_budgets represents the count of alive accounts in the three slots 0,1,2
        assert_eq!(consumed_budgets, 10);
    }

    #[test]
    fn test_upgrade_epoch() {
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_leader(500, &solana_sdk::pubkey::new_rand(), 0);
        genesis_config.fee_rate_governor = FeeRateGovernor::new(1, 0);
        let bank = Arc::new(Bank::new(&genesis_config));

        // Jump to the test-only upgrade epoch -- see `Bank::upgrade_epoch()`
        let bank = Bank::new_from_parent(
            &bank,
            &Pubkey::default(),
            genesis_config
                .epoch_schedule
                .get_first_slot_in_epoch(0xdead),
        );

        assert_eq!(bank.get_balance(&mint_keypair.pubkey()), 500);

        // Normal transfers are not allowed
        assert_eq!(
            bank.transfer(2, &mint_keypair, &mint_keypair.pubkey()),
            Err(TransactionError::ClusterMaintenance)
        );
        assert_eq!(bank.get_balance(&mint_keypair.pubkey()), 500); // no transaction fee charged

        let vote_pubkey = solana_sdk::pubkey::new_rand();
        let authorized_voter = Keypair::new();

        // VoteInstruction::Vote is allowed.  The transaction fails with a vote program instruction
        // error because the vote account is not actually setup
        let tx = Transaction::new_signed_with_payer(
            &[vote_instruction::vote(
                &vote_pubkey,
                &authorized_voter.pubkey(),
                Vote::new(vec![1], Hash::default()),
            )],
            Some(&mint_keypair.pubkey()),
            &[&mint_keypair, &authorized_voter],
            bank.last_blockhash(),
        );
        assert_eq!(
            bank.process_transaction(&tx),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::InvalidAccountOwner
            ))
        );
        assert_eq!(bank.get_balance(&mint_keypair.pubkey()), 498); // transaction fee charged

        // VoteInstruction::VoteSwitch is allowed.  The transaction fails with a vote program
        // instruction error because the vote account is not actually setup
        let tx = Transaction::new_signed_with_payer(
            &[vote_instruction::vote_switch(
                &vote_pubkey,
                &authorized_voter.pubkey(),
                Vote::new(vec![1], Hash::default()),
                Hash::default(),
            )],
            Some(&mint_keypair.pubkey()),
            &[&mint_keypair, &authorized_voter],
            bank.last_blockhash(),
        );
        assert_eq!(
            bank.process_transaction(&tx),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::InvalidAccountOwner
            ))
        );
        assert_eq!(bank.get_balance(&mint_keypair.pubkey()), 496); // transaction fee charged

        // Other vote program instructions, like VoteInstruction::UpdateCommission are not allowed
        let tx = Transaction::new_signed_with_payer(
            &[vote_instruction::update_commission(
                &vote_pubkey,
                &authorized_voter.pubkey(),
                123,
            )],
            Some(&mint_keypair.pubkey()),
            &[&mint_keypair, &authorized_voter],
            bank.last_blockhash(),
        );
        assert_eq!(
            bank.process_transaction(&tx),
            Err(TransactionError::ClusterMaintenance)
        );
        assert_eq!(bank.get_balance(&mint_keypair.pubkey()), 496); // no transaction fee charged
    }

    #[test]
    fn test_add_builtin_no_overwrite() {
        let (genesis_config, _mint_keypair) = create_genesis_config(100_000);

        #[allow(clippy::unnecessary_wraps)]
        fn mock_ix_processor(
            _pubkey: &Pubkey,
            _data: &[u8],
            _invoke_context: &mut dyn InvokeContext,
        ) -> std::result::Result<(), InstructionError> {
            Ok(())
        }

        let slot = 123;
        let program_id = solana_sdk::pubkey::new_rand();

        let mut bank = Arc::new(Bank::new_from_parent(
            &Arc::new(Bank::new(&genesis_config)),
            &Pubkey::default(),
            slot,
        ));
        assert_eq!(bank.get_account_modified_slot(&program_id), None);

        Arc::get_mut(&mut bank)
            .unwrap()
            .add_builtin("mock_program", program_id, mock_ix_processor);
        assert_eq!(bank.get_account_modified_slot(&program_id).unwrap().1, slot);

        let mut bank = Arc::new(new_from_parent(&bank));
        Arc::get_mut(&mut bank)
            .unwrap()
            .add_builtin("mock_program", program_id, mock_ix_processor);
        assert_eq!(bank.get_account_modified_slot(&program_id).unwrap().1, slot);

        Arc::get_mut(&mut bank).unwrap().replace_builtin(
            "mock_program v2",
            program_id,
            mock_ix_processor,
        );
        assert_eq!(
            bank.get_account_modified_slot(&program_id).unwrap().1,
            bank.slot()
        );
    }

    #[test]
    fn test_add_builtin_loader_no_overwrite() {
        let (genesis_config, _mint_keypair) = create_genesis_config(100_000);

        #[allow(clippy::unnecessary_wraps)]
        fn mock_ix_processor(
            _pubkey: &Pubkey,
            _data: &[u8],
            _context: &mut dyn InvokeContext,
        ) -> std::result::Result<(), InstructionError> {
            Ok(())
        }

        let slot = 123;
        let loader_id = solana_sdk::pubkey::new_rand();

        let mut bank = Arc::new(Bank::new_from_parent(
            &Arc::new(Bank::new(&genesis_config)),
            &Pubkey::default(),
            slot,
        ));
        assert_eq!(bank.get_account_modified_slot(&loader_id), None);

        Arc::get_mut(&mut bank)
            .unwrap()
            .add_builtin("mock_program", loader_id, mock_ix_processor);
        assert_eq!(bank.get_account_modified_slot(&loader_id).unwrap().1, slot);

        let mut bank = Arc::new(new_from_parent(&bank));
        Arc::get_mut(&mut bank)
            .unwrap()
            .add_builtin("mock_program", loader_id, mock_ix_processor);
        assert_eq!(bank.get_account_modified_slot(&loader_id).unwrap().1, slot);
    }

    #[test]
    fn test_reconfigure_token2_native_mint() {
        solana_logger::setup();

        let mut genesis_config =
            create_genesis_config_with_leader(5, &solana_sdk::pubkey::new_rand(), 0).genesis_config;

        // ClusterType::Development - Native mint exists immediately
        assert_eq!(genesis_config.cluster_type, ClusterType::Development);
        let bank = Arc::new(Bank::new(&genesis_config));
        assert_eq!(
            bank.get_balance(&inline_spl_token_v2_0::native_mint::id()),
            1000000000
        );

        // Testnet - Native mint blinks into existence at epoch 93
        genesis_config.cluster_type = ClusterType::Testnet;
        let bank = Arc::new(Bank::new(&genesis_config));
        assert_eq!(
            bank.get_balance(&inline_spl_token_v2_0::native_mint::id()),
            0
        );
        bank.deposit(&inline_spl_token_v2_0::native_mint::id(), 4200000000)
            .unwrap();

        let bank = Bank::new_from_parent(
            &bank,
            &Pubkey::default(),
            genesis_config.epoch_schedule.get_first_slot_in_epoch(93),
        );

        let native_mint_account = bank
            .get_account(&inline_spl_token_v2_0::native_mint::id())
            .unwrap();
        assert_eq!(native_mint_account.data().len(), 82);
        assert_eq!(
            bank.get_balance(&inline_spl_token_v2_0::native_mint::id()),
            4200000000
        );
        assert_eq!(native_mint_account.owner(), &inline_spl_token_v2_0::id());

        // MainnetBeta - Native mint blinks into existence at epoch 75
        genesis_config.cluster_type = ClusterType::MainnetBeta;
        let bank = Arc::new(Bank::new(&genesis_config));
        assert_eq!(
            bank.get_balance(&inline_spl_token_v2_0::native_mint::id()),
            0
        );
        bank.deposit(&inline_spl_token_v2_0::native_mint::id(), 4200000000)
            .unwrap();

        let bank = Bank::new_from_parent(
            &bank,
            &Pubkey::default(),
            genesis_config.epoch_schedule.get_first_slot_in_epoch(75),
        );

        let native_mint_account = bank
            .get_account(&inline_spl_token_v2_0::native_mint::id())
            .unwrap();
        assert_eq!(native_mint_account.data().len(), 82);
        assert_eq!(
            bank.get_balance(&inline_spl_token_v2_0::native_mint::id()),
            4200000000
        );
        assert_eq!(native_mint_account.owner(), &inline_spl_token_v2_0::id());
    }

    #[test]
    fn test_ensure_no_storage_rewards_pool() {
        solana_logger::setup();

        let mut genesis_config =
            create_genesis_config_with_leader(5, &solana_sdk::pubkey::new_rand(), 0).genesis_config;

        // Testnet - Storage rewards pool is purged at epoch 93
        // Also this is with bad capitalization
        genesis_config.cluster_type = ClusterType::Testnet;
        genesis_config.inflation = Inflation::default();
        let reward_pubkey = solana_sdk::pubkey::new_rand();
        genesis_config.rewards_pools.insert(
            reward_pubkey,
            Account::new(u64::MAX, 0, &solana_sdk::pubkey::new_rand()),
        );
        let bank0 = Bank::new(&genesis_config);
        // because capitalization has been reset with bogus capitalization calculation allowing overflows,
        // deliberately substract 1 lamport to simulate it
        bank0.capitalization.fetch_sub(1, Relaxed);
        let bank0 = Arc::new(bank0);
        assert_eq!(bank0.get_balance(&reward_pubkey), u64::MAX,);

        let bank1 = Bank::new_from_parent(
            &bank0,
            &Pubkey::default(),
            genesis_config.epoch_schedule.get_first_slot_in_epoch(93),
        );

        // assert that everything gets in order....
        assert!(bank1.get_account(&reward_pubkey).is_none());
        let sysvar_and_native_proram_delta = 1;
        assert_eq!(
            bank0.capitalization() + 1 + 1_000_000_000 + sysvar_and_native_proram_delta,
            bank1.capitalization()
        );
        assert_eq!(bank1.capitalization(), bank1.calculate_capitalization());

        // Depending on RUSTFLAGS, this test exposes rust's checked math behavior or not...
        // So do some convolted setup; anyway this test itself will just be temporary
        let bank0 = std::panic::AssertUnwindSafe(bank0);
        let overflowing_capitalization =
            std::panic::catch_unwind(|| bank0.calculate_capitalization());
        if let Ok(overflowing_capitalization) = overflowing_capitalization {
            info!("asserting overflowing capitalization for bank0");
            assert_eq!(overflowing_capitalization, bank0.capitalization());
        } else {
            info!("NOT-asserting overflowing capitalization for bank0");
        }
    }

    #[test]
    fn test_spl_token_v2_self_transfer_fix() {
        let (genesis_config, _mint_keypair) = create_genesis_config(0);
        let mut bank = Bank::new(&genesis_config);

        // Setup original token account
        bank.store_account_and_update_capitalization(
            &inline_spl_token_v2_0::id(),
            &AccountSharedData::from(Account {
                lamports: 100,
                ..Account::default()
            }),
        );
        assert_eq!(bank.get_balance(&inline_spl_token_v2_0::id()), 100);

        // Setup new token account
        let new_token_account = AccountSharedData::from(Account {
            lamports: 123,
            ..Account::default()
        });
        bank.store_account_and_update_capitalization(
            &inline_spl_token_v2_0::new_token_program::id(),
            &new_token_account,
        );
        assert_eq!(
            bank.get_balance(&inline_spl_token_v2_0::new_token_program::id()),
            123
        );

        let original_capitalization = bank.capitalization();

        bank.apply_spl_token_v2_self_transfer_fix();

        // New token account is now empty
        assert_eq!(
            bank.get_balance(&inline_spl_token_v2_0::new_token_program::id()),
            0
        );

        // Old token account holds the new token account
        assert_eq!(
            bank.get_account(&inline_spl_token_v2_0::id()),
            Some(new_token_account)
        );

        // Lamports in the old token account were burnt
        assert_eq!(bank.capitalization(), original_capitalization - 100);
    }

    fn poh_estimate_offset(bank: &Bank) -> Duration {
        let mut epoch_start_slot = bank.epoch_schedule.get_first_slot_in_epoch(bank.epoch());
        if epoch_start_slot == bank.slot() {
            epoch_start_slot = bank
                .epoch_schedule
                .get_first_slot_in_epoch(bank.epoch() - 1);
        }
        bank.slot().saturating_sub(epoch_start_slot) as u32
            * Duration::from_nanos(bank.ns_per_slot as u64)
    }

    #[test]
    fn test_warp_timestamp_again_feature_slow() {
        fn max_allowable_delta_since_epoch(bank: &Bank, max_allowable_drift: u32) -> i64 {
            let poh_estimate_offset = poh_estimate_offset(bank);
            (poh_estimate_offset.as_secs()
                + (poh_estimate_offset * max_allowable_drift / 100).as_secs()) as i64
        }

        let leader_pubkey = solana_sdk::pubkey::new_rand();
        let GenesisConfigInfo {
            mut genesis_config,
            voting_keypair,
            ..
        } = create_genesis_config_with_leader(5, &leader_pubkey, 3);
        let slots_in_epoch = 32;
        genesis_config
            .accounts
            .remove(&feature_set::warp_timestamp_again::id())
            .unwrap();
        genesis_config.epoch_schedule = EpochSchedule::new(slots_in_epoch);
        let mut bank = Bank::new(&genesis_config);

        let recent_timestamp: UnixTimestamp = bank.unix_timestamp_from_genesis();
        let additional_secs = 8; // Greater than MAX_ALLOWABLE_DRIFT_PERCENTAGE for full epoch
        update_vote_account_timestamp(
            BlockTimestamp {
                slot: bank.slot(),
                timestamp: recent_timestamp + additional_secs,
            },
            &bank,
            &voting_keypair.pubkey(),
        );

        // additional_secs greater than MAX_ALLOWABLE_DRIFT_PERCENTAGE for an epoch
        // timestamp bounded to 50% deviation
        for _ in 0..31 {
            bank = new_from_parent(&Arc::new(bank));
            assert_eq!(
                bank.clock().unix_timestamp,
                bank.clock().epoch_start_timestamp
                    + max_allowable_delta_since_epoch(&bank, MAX_ALLOWABLE_DRIFT_PERCENTAGE),
            );
            assert_eq!(bank.clock().epoch_start_timestamp, recent_timestamp);
        }

        // Request `warp_timestamp_again` activation
        let feature = Feature { activated_at: None };
        bank.store_account(
            &feature_set::warp_timestamp_again::id(),
            &feature::create_account(&feature, 42),
        );
        let previous_epoch_timestamp = bank.clock().epoch_start_timestamp;
        let previous_timestamp = bank.clock().unix_timestamp;

        // Advance to epoch boundary to activate; time is warped to estimate with no bounding
        bank = new_from_parent(&Arc::new(bank));
        assert_ne!(bank.clock().epoch_start_timestamp, previous_timestamp);
        assert!(
            bank.clock().epoch_start_timestamp
                > previous_epoch_timestamp
                    + max_allowable_delta_since_epoch(&bank, MAX_ALLOWABLE_DRIFT_PERCENTAGE)
        );

        // Refresh vote timestamp
        let recent_timestamp: UnixTimestamp = bank.clock().unix_timestamp;
        let additional_secs = 8;
        update_vote_account_timestamp(
            BlockTimestamp {
                slot: bank.slot(),
                timestamp: recent_timestamp + additional_secs,
            },
            &bank,
            &voting_keypair.pubkey(),
        );

        // additional_secs greater than MAX_ALLOWABLE_DRIFT_PERCENTAGE for 22 slots
        // timestamp bounded to 80% deviation
        for _ in 0..23 {
            bank = new_from_parent(&Arc::new(bank));
            assert_eq!(
                bank.clock().unix_timestamp,
                bank.clock().epoch_start_timestamp
                    + max_allowable_delta_since_epoch(&bank, MAX_ALLOWABLE_DRIFT_PERCENTAGE_SLOW),
            );
            assert_eq!(bank.clock().epoch_start_timestamp, recent_timestamp);
        }
        for _ in 0..8 {
            bank = new_from_parent(&Arc::new(bank));
            assert_eq!(
                bank.clock().unix_timestamp,
                bank.clock().epoch_start_timestamp
                    + poh_estimate_offset(&bank).as_secs() as i64
                    + additional_secs,
            );
            assert_eq!(bank.clock().epoch_start_timestamp, recent_timestamp);
        }
    }

    #[test]
    fn test_timestamp_fast() {
        fn max_allowable_delta_since_epoch(bank: &Bank, max_allowable_drift: u32) -> i64 {
            let poh_estimate_offset = poh_estimate_offset(bank);
            (poh_estimate_offset.as_secs()
                - (poh_estimate_offset * max_allowable_drift / 100).as_secs()) as i64
        }

        let leader_pubkey = solana_sdk::pubkey::new_rand();
        let GenesisConfigInfo {
            mut genesis_config,
            voting_keypair,
            ..
        } = create_genesis_config_with_leader(5, &leader_pubkey, 3);
        let slots_in_epoch = 32;
        genesis_config.epoch_schedule = EpochSchedule::new(slots_in_epoch);
        let mut bank = Bank::new(&genesis_config);

        let recent_timestamp: UnixTimestamp = bank.unix_timestamp_from_genesis();
        let additional_secs = 5; // Greater than MAX_ALLOWABLE_DRIFT_PERCENTAGE_FAST for full epoch
        update_vote_account_timestamp(
            BlockTimestamp {
                slot: bank.slot(),
                timestamp: recent_timestamp - additional_secs,
            },
            &bank,
            &voting_keypair.pubkey(),
        );

        // additional_secs greater than MAX_ALLOWABLE_DRIFT_PERCENTAGE_FAST for an epoch
        // timestamp bounded to 25% deviation
        for _ in 0..31 {
            bank = new_from_parent(&Arc::new(bank));
            assert_eq!(
                bank.clock().unix_timestamp,
                bank.clock().epoch_start_timestamp
                    + max_allowable_delta_since_epoch(&bank, MAX_ALLOWABLE_DRIFT_PERCENTAGE_FAST),
            );
            assert_eq!(bank.clock().epoch_start_timestamp, recent_timestamp);
        }
    }

    #[test]
    fn test_debug_bank() {
        let (genesis_config, _mint_keypair) = create_genesis_config(50000);
        let mut bank = Bank::new(&genesis_config);
        bank.finish_init(&genesis_config, None);
        let debug = format!("{:#?}", bank);
        assert!(!debug.is_empty());
    }

    fn test_store_scan_consistency<F: 'static>(accounts_db_caching_enabled: bool, update_f: F)
    where
        F: Fn(Arc<Bank>, crossbeam_channel::Sender<Arc<Bank>>, Arc<HashSet<Pubkey>>, Pubkey, u64)
            + std::marker::Send,
    {
        // Set up initial bank
        let mut genesis_config = create_genesis_config_with_leader(
            10,
            &solana_sdk::pubkey::new_rand(),
            374_999_998_287_840,
        )
        .genesis_config;
        genesis_config.rent = Rent::free();
        let bank0 = Arc::new(Bank::new_with_config(
            &genesis_config,
            HashSet::new(),
            accounts_db_caching_enabled,
        ));

        // Set up pubkeys to write to
        let total_pubkeys = ITER_BATCH_SIZE * 10;
        let total_pubkeys_to_modify = 10;
        let all_pubkeys: Vec<Pubkey> = std::iter::repeat_with(solana_sdk::pubkey::new_rand)
            .take(total_pubkeys)
            .collect();
        let program_id = system_program::id();
        let starting_lamports = 1;
        let starting_account = AccountSharedData::new(starting_lamports, 0, &program_id);

        // Write accounts to the store
        for key in &all_pubkeys {
            bank0.store_account(&key, &starting_account);
        }

        // Set aside a subset of accounts to modify
        let pubkeys_to_modify: Arc<HashSet<Pubkey>> = Arc::new(
            all_pubkeys
                .into_iter()
                .take(total_pubkeys_to_modify)
                .collect(),
        );
        let exit = Arc::new(AtomicBool::new(false));

        // Thread that runs scan and constantly checks for
        // consistency
        let pubkeys_to_modify_ = pubkeys_to_modify.clone();
        let exit_ = exit.clone();

        // Channel over which the bank to scan is sent
        let (bank_to_scan_sender, bank_to_scan_receiver): (
            crossbeam_channel::Sender<Arc<Bank>>,
            crossbeam_channel::Receiver<Arc<Bank>>,
        ) = bounded(1);
        let scan_thread = Builder::new()
            .name("scan".to_string())
            .spawn(move || loop {
                if exit_.load(Relaxed) {
                    return;
                }
                if let Ok(bank_to_scan) =
                    bank_to_scan_receiver.recv_timeout(Duration::from_millis(10))
                {
                    let accounts = bank_to_scan.get_program_accounts(&program_id);
                    // Should never see empty accounts because no slot ever deleted
                    // any of the original accounts, and the scan should reflect the
                    // account state at some frozen slot `X` (no partial updates).
                    assert!(!accounts.is_empty());
                    let mut expected_lamports = None;
                    let mut target_accounts_found = HashSet::new();
                    for (pubkey, account) in accounts {
                        let account_balance = account.lamports();
                        if pubkeys_to_modify_.contains(&pubkey) {
                            target_accounts_found.insert(pubkey);
                            if let Some(expected_lamports) = expected_lamports {
                                assert_eq!(account_balance, expected_lamports);
                            } else {
                                // All pubkeys in the specified set should have the same balance
                                expected_lamports = Some(account_balance);
                            }
                        }
                    }

                    // Should've found all the accounts, i.e. no partial cleans should
                    // be detected
                    assert_eq!(target_accounts_found.len(), total_pubkeys_to_modify);
                }
            })
            .unwrap();

        // Thread that constantly updates the accounts, sets
        // roots, and cleans
        let update_thread = Builder::new()
            .name("update".to_string())
            .spawn(move || {
                update_f(
                    bank0,
                    bank_to_scan_sender,
                    pubkeys_to_modify,
                    program_id,
                    starting_lamports,
                );
            })
            .unwrap();

        // Let threads run for a while, check the scans didn't see any mixed slots
        std::thread::sleep(Duration::new(5, 0));
        exit.store(true, Relaxed);
        scan_thread.join().unwrap();
        update_thread.join().unwrap();
    }

    #[test]
    fn test_store_scan_consistency_unrooted() {
        for accounts_db_caching_enabled in &[false, true] {
            test_store_scan_consistency(
                *accounts_db_caching_enabled,
                |bank0, bank_to_scan_sender, pubkeys_to_modify, program_id, starting_lamports| {
                    let mut current_major_fork_bank = bank0;
                    loop {
                        let mut current_minor_fork_bank = current_major_fork_bank.clone();
                        let num_new_banks = 2;
                        let lamports = current_minor_fork_bank.slot() + starting_lamports + 1;
                        // Modify banks on the two banks on the minor fork
                        for pubkeys_to_modify in &pubkeys_to_modify
                            .iter()
                            .chunks(pubkeys_to_modify.len() / num_new_banks)
                        {
                            current_minor_fork_bank = Arc::new(Bank::new_from_parent(
                                &current_minor_fork_bank,
                                &solana_sdk::pubkey::new_rand(),
                                current_minor_fork_bank.slot() + 2,
                            ));
                            let account = AccountSharedData::new(lamports, 0, &program_id);
                            // Write partial updates to each of the banks in the minor fork so if any of them
                            // get cleaned up, there will be keys with the wrong account value/missing.
                            for key in pubkeys_to_modify {
                                current_minor_fork_bank.store_account(key, &account);
                            }
                            current_minor_fork_bank.freeze();
                        }

                        // All the parent banks made in this iteration of the loop
                        // are currently discoverable, previous parents should have
                        // been squashed
                        assert_eq!(
                            current_minor_fork_bank.clone().parents_inclusive().len(),
                            num_new_banks + 1,
                        );

                        // `next_major_bank` needs to be sandwiched between the minor fork banks
                        // That way, after the squash(), the minor fork has the potential to see a
                        // *partial* clean of the banks < `next_major_bank`.
                        current_major_fork_bank = Arc::new(Bank::new_from_parent(
                            &current_major_fork_bank,
                            &solana_sdk::pubkey::new_rand(),
                            current_minor_fork_bank.slot() - 1,
                        ));
                        let lamports = current_major_fork_bank.slot() + starting_lamports + 1;
                        let account = AccountSharedData::new(lamports, 0, &program_id);
                        for key in pubkeys_to_modify.iter() {
                            // Store rooted updates to these pubkeys such that the minor
                            // fork updates to the same keys will be deleted by clean
                            current_major_fork_bank.store_account(key, &account);
                        }

                        // Send the last new bank to the scan thread to perform the scan.
                        // Meanwhile this thread will continually set roots on a separate fork
                        // and squash.
                        /*
                                    bank 0
                                /         \
                        minor bank 1       \
                            /         current_major_fork_bank
                        minor bank 2

                        */
                        // The capacity of the channel is 1 so that this thread will wait for the scan to finish before starting
                        // the next iteration, allowing the scan to stay in sync with these updates
                        // such that every scan will see this interruption.
                        if bank_to_scan_sender.send(current_minor_fork_bank).is_err() {
                            // Channel was disconnected, exit
                            return;
                        }
                        current_major_fork_bank.freeze();
                        current_major_fork_bank.squash();
                        // Try to get cache flush/clean to overlap with the scan
                        current_major_fork_bank.force_flush_accounts_cache();
                        current_major_fork_bank.clean_accounts(false);
                    }
                },
            )
        }
    }

    #[test]
    fn test_store_scan_consistency_root() {
        for accounts_db_caching_enabled in &[false, true] {
            test_store_scan_consistency(
                *accounts_db_caching_enabled,
                |bank0, bank_to_scan_sender, pubkeys_to_modify, program_id, starting_lamports| {
                    let mut current_bank = bank0.clone();
                    let mut prev_bank = bank0;
                    loop {
                        let lamports_this_round = current_bank.slot() + starting_lamports + 1;
                        let account = AccountSharedData::new(lamports_this_round, 0, &program_id);
                        for key in pubkeys_to_modify.iter() {
                            current_bank.store_account(key, &account);
                        }
                        current_bank.freeze();
                        // Send the previous bank to the scan thread to perform the scan.
                        // Meanwhile this thread will squash and update roots immediately after
                        // so the roots will update while scanning.
                        //
                        // The capacity of the channel is 1 so that this thread will wait for the scan to finish before starting
                        // the next iteration, allowing the scan to stay in sync with these updates
                        // such that every scan will see this interruption.
                        if bank_to_scan_sender.send(prev_bank).is_err() {
                            // Channel was disconnected, exit
                            return;
                        }
                        current_bank.squash();
                        if current_bank.slot() % 2 == 0 {
                            current_bank.force_flush_accounts_cache();
                            current_bank.clean_accounts(true);
                        }
                        prev_bank = current_bank.clone();
                        current_bank = Arc::new(Bank::new_from_parent(
                            &current_bank,
                            &solana_sdk::pubkey::new_rand(),
                            current_bank.slot() + 1,
                        ));
                    }
                },
            );
        }
    }

    #[test]
    fn test_stake_rewrite() {
        let GenesisConfigInfo { genesis_config, .. } =
            create_genesis_config_with_leader(500, &solana_sdk::pubkey::new_rand(), 1);
        let bank = Arc::new(Bank::new(&genesis_config));

        // quickest way of creting bad stake account
        let bootstrap_stake_pubkey = bank
            .cloned_stake_delegations()
            .keys()
            .next()
            .copied()
            .unwrap();
        let mut bootstrap_stake_account = bank.get_account(&bootstrap_stake_pubkey).unwrap();
        bootstrap_stake_account.set_lamports(10000000);
        bank.store_account(&bootstrap_stake_pubkey, &bootstrap_stake_account);

        assert_eq!(bank.rewrite_stakes(), (1, 1));
    }

    #[test]
    fn test_transfer_sysvar() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_leader(
            1_000_000_000_000_000,
            &Pubkey::new_unique(),
            bootstrap_validator_stake_lamports(),
        );
        let mut bank = Bank::new(&genesis_config);

        fn mock_ix_processor(
            _pubkey: &Pubkey,
            _data: &[u8],
            invoke_context: &mut dyn InvokeContext,
        ) -> std::result::Result<(), InstructionError> {
            use solana_sdk::account::WritableAccount;
            let keyed_accounts = invoke_context.get_keyed_accounts()?;
            let mut data = keyed_accounts[1].try_account_ref_mut()?;
            data.data_as_mut_slice()[0] = 5;
            Ok(())
        }

        let program_id = solana_sdk::pubkey::new_rand();
        bank.add_builtin("mock_program1", program_id, mock_ix_processor);

        let blockhash = bank.last_blockhash();
        let blockhash_sysvar = sysvar::recent_blockhashes::id();
        let orig_lamports = bank
            .get_account(&sysvar::recent_blockhashes::id())
            .unwrap()
            .lamports();
        info!("{:?}", bank.get_account(&sysvar::recent_blockhashes::id()));
        let tx = system_transaction::transfer(&mint_keypair, &blockhash_sysvar, 10, blockhash);
        assert_eq!(
            bank.process_transaction(&tx),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::ReadonlyLamportChange
            ))
        );
        assert_eq!(
            bank.get_account(&sysvar::recent_blockhashes::id())
                .unwrap()
                .lamports(),
            orig_lamports
        );
        info!("{:?}", bank.get_account(&sysvar::recent_blockhashes::id()));

        let accounts = vec![
            AccountMeta::new(mint_keypair.pubkey(), true),
            AccountMeta::new(blockhash_sysvar, false),
        ];
        let ix = Instruction::new_with_bincode(program_id, &0, accounts);
        let message = Message::new(&[ix], Some(&mint_keypair.pubkey()));
        let tx = Transaction::new(&[&mint_keypair], message, blockhash);
        assert_eq!(
            bank.process_transaction(&tx),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::ReadonlyDataModified
            ))
        );
    }
}
