use std::{
    cell::RefCell,
    collections::HashSet,
    rc::Rc,
    sync::atomic::Ordering::Relaxed,
    sync::{Arc, RwLock},
};

use super::{
    Bank, BankSlotDelta, BankStatusCache, TransactionAccountDepRefCells,
    TransactionAccountRefCells, TransactionLoaderRefCells,
};
use crate::{
    accounts::{
        AccountAddressFilter, Accounts, TransactionAccountDeps, TransactionAccounts,
        TransactionLoaders,
    },
    accounts_index::{Ancestors, IndexKey},
    serde_snapshot::SnapshotStorages,
    stakes::Stakes,
};
use itertools::Itertools;
#[cfg(RUSTC_WITH_SPECIALIZATION)]
use solana_frozen_abi::abi_example::AbiExample;
use solana_sdk::{
    account::{AccountSharedData, ReadableAccount},
    clock::Slot,
    pubkey::Pubkey,
    transaction::TransactionError,
};

#[derive(Default, Debug)]
pub struct BankRc {
    /// where all the Accounts are stored
    pub accounts: Arc<Accounts>,

    /// Previous checkpoint of this bank
    pub(crate) parent: RwLock<Option<Arc<Bank>>>,

    /// Current slot
    pub(crate) slot: Slot,
}

#[cfg(RUSTC_WITH_SPECIALIZATION)]
impl AbiExample for BankRc {
    fn example() -> Self {
        BankRc {
            // Set parent to None to cut the recursion into another Bank
            parent: RwLock::new(None),
            // AbiExample for Accounts is specially implemented to contain a storage example
            accounts: AbiExample::example(),
            slot: AbiExample::example(),
        }
    }
}

impl BankRc {
    pub(crate) fn new(accounts: Accounts, slot: Slot) -> Self {
        Self {
            accounts: Arc::new(accounts),
            parent: RwLock::new(None),
            slot,
        }
    }

    pub fn get_snapshot_storages(&self, slot: Slot) -> SnapshotStorages {
        self.accounts.accounts_db.get_snapshot_storages(slot)
    }
}

#[derive(Default, Debug, AbiExample)]
pub struct StatusCacheRc {
    /// where all the Accounts are stored
    /// A cache of signature statuses
    pub status_cache: Arc<RwLock<BankStatusCache>>,
}

impl StatusCacheRc {
    pub fn slot_deltas(&self, slots: &[Slot]) -> Vec<BankSlotDelta> {
        let sc = self.status_cache.read().unwrap();
        sc.slot_deltas(slots)
    }

    pub fn roots(&self) -> Vec<Slot> {
        self.status_cache
            .read()
            .unwrap()
            .roots()
            .iter()
            .cloned()
            .sorted()
            .collect()
    }

    pub fn append(&self, slot_deltas: &[BankSlotDelta]) {
        let mut sc = self.status_cache.write().unwrap();
        sc.append(slot_deltas);
    }
}

impl Bank {
    pub fn accounts(&self) -> Arc<Accounts> {
        self.rc.accounts.clone()
    }

    pub fn store_account(&self, pubkey: &Pubkey, account: &AccountSharedData) {
        assert!(!self.freeze_started());
        self.rc
            .accounts
            .store_slow_cached(self.slot(), pubkey, account);

        if Stakes::is_stake(account) {
            self.stakes.write().unwrap().store(
                pubkey,
                account,
                self.stake_program_v2_enabled(),
                self.check_init_vote_data_enabled(),
            );
        }
    }

    /// Technically this issues (or even burns!) new lamports,
    /// so be extra careful for its usage
    pub(super) fn store_account_and_update_capitalization(
        &self,
        pubkey: &Pubkey,
        new_account: &AccountSharedData,
    ) {
        if let Some(old_account) = self.get_account_with_fixed_root(&pubkey) {
            match new_account.lamports().cmp(&old_account.lamports()) {
                std::cmp::Ordering::Greater => {
                    self.capitalization
                        .fetch_add(new_account.lamports() - old_account.lamports(), Relaxed);
                }
                std::cmp::Ordering::Less => {
                    self.capitalization
                        .fetch_sub(old_account.lamports() - new_account.lamports(), Relaxed);
                }
                std::cmp::Ordering::Equal => {}
            }
        } else {
            self.capitalization
                .fetch_add(new_account.lamports(), Relaxed);
        }

        self.store_account(pubkey, new_account);
    }

    // Hi! leaky abstraction here....
    // try to use get_account_with_fixed_root() if it's called ONLY from on-chain runtime account
    // processing. That alternative fn provides more safety.
    pub fn get_account(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        self.get_account_modified_slot(pubkey)
            .map(|(acc, _slot)| acc)
    }

    // Hi! leaky abstraction here....
    // use this over get_account() if it's called ONLY from on-chain runtime account
    // processing (i.e. from in-band replay/banking stage; that ensures root is *fixed* while
    // running).
    // pro: safer assertion can be enabled inside AccountsDb
    // con: panics!() if called from off-chain processing
    pub fn get_account_with_fixed_root(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        self.load_slow_with_fixed_root(&self.ancestors, pubkey)
            .map(|(acc, _slot)| acc)
    }

    pub fn get_account_modified_slot(&self, pubkey: &Pubkey) -> Option<(AccountSharedData, Slot)> {
        self.load_slow(&self.ancestors, pubkey)
    }

    fn load_slow(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
    ) -> Option<(AccountSharedData, Slot)> {
        // get_account (= primary this fn caller) may be called from on-chain Bank code even if we
        // try hard to use get_account_with_fixed_root for that purpose...
        // so pass safer LoadHint:Unspecified here as a fallback
        self.rc.accounts.load_without_fixed_root(ancestors, pubkey)
    }

    fn load_slow_with_fixed_root(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
    ) -> Option<(AccountSharedData, Slot)> {
        self.rc.accounts.load_with_fixed_root(ancestors, pubkey)
    }

    // Exclude self to really fetch the parent Bank's account hash and data.
    //
    // Being idempotent is needed to make the lazy initialization possible,
    // especially for update_slot_hashes at the moment, which can be called
    // multiple times with the same parent_slot in the case of forking.
    //
    // Generally, all of sysvar update granularity should be slot boundaries.
    pub(super) fn get_sysvar_account_with_fixed_root(
        &self,
        pubkey: &Pubkey,
    ) -> Option<AccountSharedData> {
        let mut ancestors = self.ancestors.clone();
        ancestors.remove(&self.slot());
        self.rc
            .accounts
            .load_with_fixed_root(&ancestors, pubkey)
            .map(|(acc, _slot)| acc)
    }

    pub fn get_program_accounts(&self, program_id: &Pubkey) -> Vec<(Pubkey, AccountSharedData)> {
        self.rc
            .accounts
            .load_by_program(&self.ancestors, program_id)
    }

    pub fn get_filtered_program_accounts<F: Fn(&AccountSharedData) -> bool>(
        &self,
        program_id: &Pubkey,
        filter: F,
    ) -> Vec<(Pubkey, AccountSharedData)> {
        self.rc
            .accounts
            .load_by_program_with_filter(&self.ancestors, program_id, filter)
    }

    pub fn get_filtered_indexed_accounts<F: Fn(&AccountSharedData) -> bool>(
        &self,
        index_key: &IndexKey,
        filter: F,
    ) -> Vec<(Pubkey, AccountSharedData)> {
        self.rc
            .accounts
            .load_by_index_key_with_filter(&self.ancestors, index_key, filter)
    }

    pub fn get_all_accounts_with_modified_slots(&self) -> Vec<(Pubkey, AccountSharedData, Slot)> {
        self.rc.accounts.load_all(&self.ancestors)
    }

    pub fn get_program_accounts_modified_since_parent(
        &self,
        program_id: &Pubkey,
    ) -> Vec<(Pubkey, AccountSharedData)> {
        self.rc
            .accounts
            .load_by_program_slot(self.slot(), Some(program_id))
    }

    pub fn get_all_accounts_modified_since_parent(&self) -> Vec<(Pubkey, AccountSharedData)> {
        self.rc.accounts.load_by_program_slot(self.slot(), None)
    }

    // if you want get_account_modified_since_parent without fixed_root, please define so...
    pub(super) fn get_account_modified_since_parent_with_fixed_root(
        &self,
        pubkey: &Pubkey,
    ) -> Option<(AccountSharedData, Slot)> {
        let just_self: Ancestors = vec![(self.slot(), 0)].into_iter().collect();
        if let Some((account, slot)) = self.load_slow_with_fixed_root(&just_self, pubkey) {
            if slot == self.slot() {
                return Some((account, slot));
            }
        }
        None
    }

    pub fn get_largest_accounts(
        &self,
        num: usize,
        filter_by_address: &HashSet<Pubkey>,
        filter: AccountAddressFilter,
    ) -> Vec<(Pubkey, u64)> {
        self.rc
            .accounts
            .load_largest_accounts(&self.ancestors, num, filter_by_address, filter)
    }
    /// Converts Accounts into RefCell<AccountSharedData>, this involves moving
    /// ownership by draining the source
    pub(super) fn accounts_to_refcells(
        accounts: &mut TransactionAccounts,
        account_deps: &mut TransactionAccountDeps,
        loaders: &mut TransactionLoaders,
    ) -> (
        TransactionAccountRefCells,
        TransactionAccountDepRefCells,
        TransactionLoaderRefCells,
    ) {
        let account_refcells: Vec<_> = accounts
            .drain(..)
            .map(|account| Rc::new(RefCell::new(account)))
            .collect();
        let account_dep_refcells: Vec<_> = account_deps
            .drain(..)
            .map(|(pubkey, account_dep)| (pubkey, Rc::new(RefCell::new(account_dep))))
            .collect();
        let loader_refcells: Vec<Vec<_>> = loaders
            .iter_mut()
            .map(|v| {
                v.drain(..)
                    .map(|(pubkey, account)| (pubkey, Rc::new(RefCell::new(account))))
                    .collect()
            })
            .collect();
        (account_refcells, account_dep_refcells, loader_refcells)
    }

    /// Converts back from RefCell<AccountSharedData> to AccountSharedData, this involves moving
    /// ownership by draining the sources
    pub(super) fn refcells_to_accounts(
        accounts: &mut TransactionAccounts,
        loaders: &mut TransactionLoaders,
        mut account_refcells: TransactionAccountRefCells,
        loader_refcells: TransactionLoaderRefCells,
    ) -> std::result::Result<(), TransactionError> {
        for account_refcell in account_refcells.drain(..) {
            accounts.push(
                Rc::try_unwrap(account_refcell)
                    .map_err(|_| TransactionError::AccountBorrowOutstanding)?
                    .into_inner(),
            )
        }
        for (ls, mut lrcs) in loaders.iter_mut().zip(loader_refcells) {
            for (pubkey, lrc) in lrcs.drain(..) {
                ls.push((
                    pubkey,
                    Rc::try_unwrap(lrc)
                        .map_err(|_| TransactionError::AccountBorrowOutstanding)?
                        .into_inner(),
                ))
            }
        }

        Ok(())
    }

    pub fn clean_accounts(&self, skip_last: bool) {
        let max_clean_slot = if skip_last {
            // Don't clean the slot we're snapshotting because it may have zero-lamport
            // accounts that were included in the bank delta hash when the bank was frozen,
            // and if we clean them here, any newly created snapshot's hash for this bank
            // may not match the frozen hash.
            Some(self.slot().saturating_sub(1))
        } else {
            None
        };
        self.rc.accounts.accounts_db.clean_accounts(max_clean_slot);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        accounts_index::AccountIndex,
        bank::test_utils::{assert_capitalization_diff, new_from_parent},
        genesis_utils::{create_genesis_config_with_leader, GenesisConfigInfo},
    };
    use solana_sdk::{
        genesis_config::create_genesis_config, native_token::sol_to_lamports, signature::Signer,
        system_program,
    };
    #[test]
    fn test_store_account_and_update_capitalization_missing() {
        let (genesis_config, _mint_keypair) = create_genesis_config(0);
        let bank = Bank::new(&genesis_config);
        let pubkey = solana_sdk::pubkey::new_rand();

        let some_lamports = 400;
        let account = AccountSharedData::new(some_lamports, 0, &system_program::id());

        assert_capitalization_diff(
            &bank,
            || bank.store_account_and_update_capitalization(&pubkey, &account),
            |old, new| assert_eq!(old + some_lamports, new),
        );
        assert_eq!(account, bank.get_account(&pubkey).unwrap());
    }

    #[test]
    fn test_store_account_and_update_capitalization_increased() {
        let old_lamports = 400;
        let (genesis_config, mint_keypair) = create_genesis_config(old_lamports);
        let bank = Bank::new(&genesis_config);
        let pubkey = mint_keypair.pubkey();

        let new_lamports = 500;
        let account = AccountSharedData::new(new_lamports, 0, &system_program::id());

        assert_capitalization_diff(
            &bank,
            || bank.store_account_and_update_capitalization(&pubkey, &account),
            |old, new| assert_eq!(old + 100, new),
        );
        assert_eq!(account, bank.get_account(&pubkey).unwrap());
    }

    #[test]
    fn test_store_account_and_update_capitalization_decreased() {
        let old_lamports = 400;
        let (genesis_config, mint_keypair) = create_genesis_config(old_lamports);
        let bank = Bank::new(&genesis_config);
        let pubkey = mint_keypair.pubkey();

        let new_lamports = 100;
        let account = AccountSharedData::new(new_lamports, 0, &system_program::id());

        assert_capitalization_diff(
            &bank,
            || bank.store_account_and_update_capitalization(&pubkey, &account),
            |old, new| assert_eq!(old - 300, new),
        );
        assert_eq!(account, bank.get_account(&pubkey).unwrap());
    }

    #[test]
    fn test_store_account_and_update_capitalization_unchanged() {
        let lamports = 400;
        let (genesis_config, mint_keypair) = create_genesis_config(lamports);
        let bank = Bank::new(&genesis_config);
        let pubkey = mint_keypair.pubkey();

        let account = AccountSharedData::new(lamports, 1, &system_program::id());

        assert_capitalization_diff(
            &bank,
            || bank.store_account_and_update_capitalization(&pubkey, &account),
            |old, new| assert_eq!(old, new),
        );
        assert_eq!(account, bank.get_account(&pubkey).unwrap());
    }

    #[test]
    fn test_bank_get_program_accounts() {
        let (genesis_config, mint_keypair) = create_genesis_config(500);
        let parent = Arc::new(Bank::new(&genesis_config));
        parent.restore_old_behavior_for_fragile_tests();

        let genesis_accounts: Vec<_> = parent.get_all_accounts_with_modified_slots();
        assert!(
            genesis_accounts
                .iter()
                .any(|(pubkey, _, _)| *pubkey == mint_keypair.pubkey()),
            "mint pubkey not found"
        );
        assert!(
            genesis_accounts
                .iter()
                .any(|(pubkey, _, _)| solana_sdk::sysvar::is_sysvar_id(pubkey)),
            "no sysvars found"
        );

        let bank0 = Arc::new(new_from_parent(&parent));
        let pubkey0 = solana_sdk::pubkey::new_rand();
        let program_id = Pubkey::new(&[2; 32]);
        let account0 = AccountSharedData::new(1, 0, &program_id);
        bank0.store_account(&pubkey0, &account0);

        assert_eq!(
            bank0.get_program_accounts_modified_since_parent(&program_id),
            vec![(pubkey0, account0.clone())]
        );

        let bank1 = Arc::new(new_from_parent(&bank0));
        bank1.squash();
        assert_eq!(
            bank0.get_program_accounts(&program_id),
            vec![(pubkey0, account0.clone())]
        );
        assert_eq!(
            bank1.get_program_accounts(&program_id),
            vec![(pubkey0, account0)]
        );
        assert_eq!(
            bank1.get_program_accounts_modified_since_parent(&program_id),
            vec![]
        );

        let bank2 = Arc::new(new_from_parent(&bank1));
        let pubkey1 = solana_sdk::pubkey::new_rand();
        let account1 = AccountSharedData::new(3, 0, &program_id);
        bank2.store_account(&pubkey1, &account1);
        // Accounts with 0 lamports should be filtered out by Accounts::load_by_program()
        let pubkey2 = solana_sdk::pubkey::new_rand();
        let account2 = AccountSharedData::new(0, 0, &program_id);
        bank2.store_account(&pubkey2, &account2);

        let bank3 = Arc::new(new_from_parent(&bank2));
        bank3.squash();
        assert_eq!(bank1.get_program_accounts(&program_id).len(), 2);
        assert_eq!(bank3.get_program_accounts(&program_id).len(), 2);
    }

    #[test]
    fn test_get_filtered_indexed_accounts() {
        let (genesis_config, _mint_keypair) = create_genesis_config(500);
        let mut account_indexes = HashSet::new();
        account_indexes.insert(AccountIndex::ProgramId);
        let bank = Arc::new(Bank::new_with_config(
            &genesis_config,
            account_indexes,
            false,
        ));

        let address = Pubkey::new_unique();
        let program_id = Pubkey::new_unique();
        let account = AccountSharedData::new(1, 0, &program_id);
        bank.store_account(&address, &account);

        let indexed_accounts =
            bank.get_filtered_indexed_accounts(&IndexKey::ProgramId(program_id), |_| true);
        assert_eq!(indexed_accounts.len(), 1);
        assert_eq!(indexed_accounts[0], (address, account));

        // Even though the account is re-stored in the bank (and the index) under a new program id,
        // it is still present in the index under the original program id as well. This
        // demonstrates the need for a redundant post-processing filter.
        let another_program_id = Pubkey::new_unique();
        let new_account = AccountSharedData::new(1, 0, &another_program_id);
        let bank = Arc::new(new_from_parent(&bank));
        bank.store_account(&address, &new_account);
        let indexed_accounts =
            bank.get_filtered_indexed_accounts(&IndexKey::ProgramId(program_id), |_| true);
        assert_eq!(indexed_accounts.len(), 1);
        assert_eq!(indexed_accounts[0], (address, new_account.clone()));
        let indexed_accounts =
            bank.get_filtered_indexed_accounts(&IndexKey::ProgramId(another_program_id), |_| true);
        assert_eq!(indexed_accounts.len(), 1);
        assert_eq!(indexed_accounts[0], (address, new_account.clone()));

        // Post-processing filter
        let indexed_accounts = bank
            .get_filtered_indexed_accounts(&IndexKey::ProgramId(program_id), |account| {
                account.owner() == &program_id
            });
        assert!(indexed_accounts.is_empty());
        let indexed_accounts = bank
            .get_filtered_indexed_accounts(&IndexKey::ProgramId(another_program_id), |account| {
                account.owner() == &another_program_id
            });
        assert_eq!(indexed_accounts.len(), 1);
        assert_eq!(indexed_accounts[0], (address, new_account));
    }

    #[test]
    fn test_get_largest_accounts() {
        let GenesisConfigInfo { genesis_config, .. } =
            create_genesis_config_with_leader(42, &solana_sdk::pubkey::new_rand(), 42);
        let bank = Bank::new(&genesis_config);

        let pubkeys: Vec<_> = (0..5).map(|_| Pubkey::new_unique()).collect();
        let pubkeys_hashset: HashSet<_> = pubkeys.iter().cloned().collect();

        let pubkeys_balances: Vec<_> = pubkeys
            .iter()
            .cloned()
            .zip(vec![
                sol_to_lamports(2.0),
                sol_to_lamports(3.0),
                sol_to_lamports(3.0),
                sol_to_lamports(4.0),
                sol_to_lamports(5.0),
            ])
            .collect();

        // Initialize accounts; all have larger SOL balances than current Bank built-ins
        let account0 = AccountSharedData::new(pubkeys_balances[0].1, 0, &Pubkey::default());
        bank.store_account(&pubkeys_balances[0].0, &account0);
        let account1 = AccountSharedData::new(pubkeys_balances[1].1, 0, &Pubkey::default());
        bank.store_account(&pubkeys_balances[1].0, &account1);
        let account2 = AccountSharedData::new(pubkeys_balances[2].1, 0, &Pubkey::default());
        bank.store_account(&pubkeys_balances[2].0, &account2);
        let account3 = AccountSharedData::new(pubkeys_balances[3].1, 0, &Pubkey::default());
        bank.store_account(&pubkeys_balances[3].0, &account3);
        let account4 = AccountSharedData::new(pubkeys_balances[4].1, 0, &Pubkey::default());
        bank.store_account(&pubkeys_balances[4].0, &account4);

        // Create HashSet to exclude an account
        let exclude4: HashSet<_> = pubkeys[4..].iter().cloned().collect();

        let mut sorted_accounts = pubkeys_balances.clone();
        sorted_accounts.sort_by(|a, b| a.1.cmp(&b.1).reverse());

        // Return only one largest account
        assert_eq!(
            bank.get_largest_accounts(1, &pubkeys_hashset, AccountAddressFilter::Include),
            vec![(pubkeys[4], sol_to_lamports(5.0))]
        );
        assert_eq!(
            bank.get_largest_accounts(1, &HashSet::new(), AccountAddressFilter::Exclude),
            vec![(pubkeys[4], sol_to_lamports(5.0))]
        );
        assert_eq!(
            bank.get_largest_accounts(1, &exclude4, AccountAddressFilter::Exclude),
            vec![(pubkeys[3], sol_to_lamports(4.0))]
        );

        // Return all added accounts
        let results =
            bank.get_largest_accounts(10, &pubkeys_hashset, AccountAddressFilter::Include);
        assert_eq!(results.len(), sorted_accounts.len());
        for pubkey_balance in sorted_accounts.iter() {
            assert!(results.contains(pubkey_balance));
        }
        let mut sorted_results = results.clone();
        sorted_results.sort_by(|a, b| a.1.cmp(&b.1).reverse());
        assert_eq!(sorted_results, results);

        let expected_accounts = sorted_accounts[1..].to_vec();
        let results = bank.get_largest_accounts(10, &exclude4, AccountAddressFilter::Exclude);
        // results include 5 Bank builtins
        assert_eq!(results.len(), 10);
        for pubkey_balance in expected_accounts.iter() {
            assert!(results.contains(pubkey_balance));
        }
        let mut sorted_results = results.clone();
        sorted_results.sort_by(|a, b| a.1.cmp(&b.1).reverse());
        assert_eq!(sorted_results, results);

        // Return 3 added accounts
        let expected_accounts = sorted_accounts[0..4].to_vec();
        let results = bank.get_largest_accounts(4, &pubkeys_hashset, AccountAddressFilter::Include);
        assert_eq!(results.len(), expected_accounts.len());
        for pubkey_balance in expected_accounts.iter() {
            assert!(results.contains(pubkey_balance));
        }

        let expected_accounts = expected_accounts[1..4].to_vec();
        let results = bank.get_largest_accounts(3, &exclude4, AccountAddressFilter::Exclude);
        assert_eq!(results.len(), expected_accounts.len());
        for pubkey_balance in expected_accounts.iter() {
            assert!(results.contains(pubkey_balance));
        }

        // Exclude more, and non-sequential, accounts
        let exclude: HashSet<_> = vec![pubkeys[0], pubkeys[2], pubkeys[4]]
            .iter()
            .cloned()
            .collect();
        assert_eq!(
            bank.get_largest_accounts(2, &exclude, AccountAddressFilter::Exclude),
            vec![pubkeys_balances[3], pubkeys_balances[1]]
        );
    }
}
