impl Bank {
    // Should not be called outside of startup, will race with
    // concurrent cleaning logic in AccountsBackgroundService
    pub fn exhaustively_free_unused_resource(&self) {
        let mut flush = Measure::start("flush");
        // Flush all the rooted accounts. Must be called after `squash()`,
        // so that AccountsDb knows what the roots are.
        self.force_flush_accounts_cache();
        flush.stop();

        let mut clean = Measure::start("clean");
        // Don't clean the slot we're snapshotting because it may have zero-lamport
        // accounts that were included in the bank delta hash when the bank was frozen,
        // and if we clean them here, any newly created snapshot's hash for this bank
        // may not match the frozen hash.
        self.clean_accounts(true);
        clean.stop();

        let mut shrink = Measure::start("shrink");
        self.shrink_all_slots();
        shrink.stop();

        info!(
            "exhaustively_free_unused_resource() {} {} {}",
            flush, clean, shrink,
        );
    }

    pub fn force_flush_accounts_cache(&self) {
        self.rc
            .accounts
            .accounts_db
            .flush_accounts_cache(true, Some(self.slot()))
    }

    pub fn flush_accounts_cache_if_needed(&self) {
        self.rc
            .accounts
            .accounts_db
            .flush_accounts_cache(false, Some(self.slot()))
    }

    /// Bank cleanup
    ///
    /// If the bank is unfrozen and then dropped, additional cleanup is needed.  In particular,
    /// cleaning up the pubkeys that are only in this bank.  To do that, call into AccountsDb to
    /// scan for dirty pubkeys and add them to the uncleaned pubkeys list so they will be cleaned
    /// up in AccountsDb::clean_accounts().
    fn cleanup(&self) {
        if self.is_frozen() {
            // nothing to do here
            return;
        }

        self.rc
            .accounts
            .accounts_db
            .scan_slot_and_insert_dirty_pubkeys_into_uncleaned_pubkeys(self.slot);
    }

    /// squash the parent's state up into this Bank,
    ///   this Bank becomes a root
    pub fn squash(&self) {
        self.freeze();

        //this bank and all its parents are now on the rooted path
        let mut roots = vec![self.slot()];
        roots.append(&mut self.parents().iter().map(|p| p.slot()).collect());

        let mut squash_accounts_time = Measure::start("squash_accounts_time");
        for slot in roots.iter().rev() {
            // root forks cannot be purged
            self.rc.accounts.add_root(*slot);
        }
        squash_accounts_time.stop();

        *self.rc.parent.write().unwrap() = None;

        let mut squash_cache_time = Measure::start("squash_cache_time");
        roots
            .iter()
            .for_each(|slot| self.src.status_cache.write().unwrap().add_root(*slot));
        squash_cache_time.stop();

        datapoint_debug!(
            "tower-observed",
            ("squash_accounts_ms", squash_accounts_time.as_ms(), i64),
            ("squash_cache_ms", squash_cache_time.as_ms(), i64)
        );
    }

    pub fn freeze(&self) {
        // This lock prevents any new commits from BankingStage
        // `process_and_record_transactions_locked()` from coming
        // in after the last tick is observed. This is because in
        // BankingStage, any transaction successfully recorded in
        // `record_transactions()` is recorded after this `hash` lock
        // is grabbed. At the time of the successful record,
        // this means the PoH has not yet reached the last tick,
        // so this means freeze() hasn't been called yet. And because
        // BankingStage doesn't release this hash lock until both
        // record and commit are finished, those transactions will be
        // committed before this write lock can be obtained here.
        let mut hash = self.hash.write().unwrap();
        if *hash == Hash::default() {
            // finish up any deferred changes to account state
            self.collect_rent_eagerly();
            self.collect_fees();
            self.distribute_rent();
            self.update_slot_history();
            self.run_incinerator();

            // freeze is a one-way trip, idempotent
            self.freeze_started.store(true, Relaxed);
            *hash = self.hash_internal_state();
            self.rc.accounts.accounts_db.mark_slot_frozen(self.slot());
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    // Test that purging 0 lamports accounts works.
    #[test]
    fn test_purge_empty_accounts() {
        solana_logger::setup();
        let (genesis_config, mint_keypair) = create_genesis_config(500_000);
        let parent = Arc::new(Bank::new(&genesis_config));
        let mut bank = parent;
        for _ in 0..10 {
            let blockhash = bank.last_blockhash();
            let pubkey = solana_sdk::pubkey::new_rand();
            let tx = system_transaction::transfer(&mint_keypair, &pubkey, 0, blockhash);
            bank.process_transaction(&tx).unwrap();
            bank.freeze();
            bank.squash();
            bank = Arc::new(new_from_parent(&bank));
        }

        bank.freeze();
        bank.squash();
        bank.force_flush_accounts_cache();
        let hash = bank.update_accounts_hash();
        bank.clean_accounts(false);
        assert_eq!(bank.update_accounts_hash(), hash);

        let bank0 = Arc::new(new_from_parent(&bank));
        let blockhash = bank.last_blockhash();
        let keypair = Keypair::new();
        let tx = system_transaction::transfer(&mint_keypair, &keypair.pubkey(), 10, blockhash);
        bank0.process_transaction(&tx).unwrap();

        let bank1 = Arc::new(new_from_parent(&bank0));
        let pubkey = solana_sdk::pubkey::new_rand();
        let blockhash = bank.last_blockhash();
        let tx = system_transaction::transfer(&keypair, &pubkey, 10, blockhash);
        bank1.process_transaction(&tx).unwrap();

        assert_eq!(bank0.get_account(&keypair.pubkey()).unwrap().lamports(), 10);
        assert_eq!(bank1.get_account(&keypair.pubkey()), None);

        info!("bank0 purge");
        let hash = bank0.update_accounts_hash();
        bank0.clean_accounts(false);
        assert_eq!(bank0.update_accounts_hash(), hash);

        assert_eq!(bank0.get_account(&keypair.pubkey()).unwrap().lamports(), 10);
        assert_eq!(bank1.get_account(&keypair.pubkey()), None);

        info!("bank1 purge");
        bank1.clean_accounts(false);

        assert_eq!(bank0.get_account(&keypair.pubkey()).unwrap().lamports(), 10);
        assert_eq!(bank1.get_account(&keypair.pubkey()), None);

        assert!(bank0.verify_bank_hash());

        // Squash and then verify hash_internal value
        bank0.freeze();
        bank0.squash();
        assert!(bank0.verify_bank_hash());

        bank1.freeze();
        bank1.squash();
        bank1.update_accounts_hash();
        assert!(bank1.verify_bank_hash());

        // keypair should have 0 tokens on both forks
        assert_eq!(bank0.get_account(&keypair.pubkey()), None);
        assert_eq!(bank1.get_account(&keypair.pubkey()), None);
        bank1.force_flush_accounts_cache();
        bank1.clean_accounts(false);

        assert!(bank1.verify_bank_hash());
    }

    /// Verifies that last ids and accounts are correctly referenced from parent
    #[test]
    fn test_bank_squash() {
        solana_logger::setup();
        let (genesis_config, mint_keypair) = create_genesis_config(2);
        let key1 = Keypair::new();
        let key2 = Keypair::new();
        let parent = Arc::new(Bank::new(&genesis_config));

        let tx_transfer_mint_to_1 =
            system_transaction::transfer(&mint_keypair, &key1.pubkey(), 1, genesis_config.hash());
        trace!("parent process tx ");
        assert_eq!(parent.process_transaction(&tx_transfer_mint_to_1), Ok(()));
        trace!("done parent process tx ");
        assert_eq!(parent.transaction_count(), 1);
        assert_eq!(
            parent.get_signature_status(&tx_transfer_mint_to_1.signatures[0]),
            Some(Ok(()))
        );

        trace!("new from parent");
        let bank = new_from_parent(&parent);
        trace!("done new from parent");
        assert_eq!(
            bank.get_signature_status(&tx_transfer_mint_to_1.signatures[0]),
            Some(Ok(()))
        );

        assert_eq!(bank.transaction_count(), parent.transaction_count());
        let tx_transfer_1_to_2 =
            system_transaction::transfer(&key1, &key2.pubkey(), 1, genesis_config.hash());
        assert_eq!(bank.process_transaction(&tx_transfer_1_to_2), Ok(()));
        assert_eq!(bank.transaction_count(), 2);
        assert_eq!(parent.transaction_count(), 1);
        assert_eq!(
            parent.get_signature_status(&tx_transfer_1_to_2.signatures[0]),
            None
        );

        for _ in 0..3 {
            // first time these should match what happened above, assert that parents are ok
            assert_eq!(bank.get_balance(&key1.pubkey()), 0);
            assert_eq!(bank.get_account(&key1.pubkey()), None);
            assert_eq!(bank.get_balance(&key2.pubkey()), 1);
            trace!("start");
            assert_eq!(
                bank.get_signature_status(&tx_transfer_mint_to_1.signatures[0]),
                Some(Ok(()))
            );
            assert_eq!(
                bank.get_signature_status(&tx_transfer_1_to_2.signatures[0]),
                Some(Ok(()))
            );

            // works iteration 0, no-ops on iteration 1 and 2
            trace!("SQUASH");
            bank.squash();

            assert_eq!(parent.transaction_count(), 1);
            assert_eq!(bank.transaction_count(), 2);
        }
    }

    #[test]
    fn test_bank_get_account_in_parent_after_squash() {
        let (genesis_config, mint_keypair) = create_genesis_config(500);
        let parent = Arc::new(Bank::new(&genesis_config));

        let key1 = Keypair::new();

        parent.transfer(1, &mint_keypair, &key1.pubkey()).unwrap();
        assert_eq!(parent.get_balance(&key1.pubkey()), 1);
        let bank = new_from_parent(&parent);
        bank.squash();
        assert_eq!(parent.get_balance(&key1.pubkey()), 1);
    }

    #[test]
    fn test_bank_get_account_in_parent_after_squash2() {
        solana_logger::setup();
        let (genesis_config, mint_keypair) = create_genesis_config(500);
        let bank0 = Arc::new(Bank::new(&genesis_config));

        let key1 = Keypair::new();

        bank0.transfer(1, &mint_keypair, &key1.pubkey()).unwrap();
        assert_eq!(bank0.get_balance(&key1.pubkey()), 1);

        let bank1 = Arc::new(Bank::new_from_parent(&bank0, &Pubkey::default(), 1));
        bank1.transfer(3, &mint_keypair, &key1.pubkey()).unwrap();
        let bank2 = Arc::new(Bank::new_from_parent(&bank0, &Pubkey::default(), 2));
        bank2.transfer(2, &mint_keypair, &key1.pubkey()).unwrap();
        let bank3 = Arc::new(Bank::new_from_parent(&bank1, &Pubkey::default(), 3));
        bank1.squash();

        // This picks up the values from 1 which is the highest root:
        // TODO: if we need to access rooted banks older than this,
        // need to fix the lookup.
        assert_eq!(bank0.get_balance(&key1.pubkey()), 4);
        assert_eq!(bank3.get_balance(&key1.pubkey()), 4);
        assert_eq!(bank2.get_balance(&key1.pubkey()), 3);
        bank3.squash();
        assert_eq!(bank1.get_balance(&key1.pubkey()), 4);

        let bank4 = Arc::new(Bank::new_from_parent(&bank3, &Pubkey::default(), 4));
        bank4.transfer(4, &mint_keypair, &key1.pubkey()).unwrap();
        assert_eq!(bank4.get_balance(&key1.pubkey()), 8);
        assert_eq!(bank3.get_balance(&key1.pubkey()), 4);
        bank4.squash();
        let bank5 = Arc::new(Bank::new_from_parent(&bank4, &Pubkey::default(), 5));
        bank5.squash();
        let bank6 = Arc::new(Bank::new_from_parent(&bank5, &Pubkey::default(), 6));
        bank6.squash();

        // This picks up the values from 4 which is the highest root:
        // TODO: if we need to access rooted banks older than this,
        // need to fix the lookup.
        assert_eq!(bank3.get_balance(&key1.pubkey()), 8);
        assert_eq!(bank2.get_balance(&key1.pubkey()), 8);

        assert_eq!(bank4.get_balance(&key1.pubkey()), 8);
    }

    #[test]
    fn test_hash_internal_state_error() {
        solana_logger::setup();
        let (genesis_config, mint_keypair) = create_genesis_config(100);
        let bank = Bank::new(&genesis_config);
        let key0 = solana_sdk::pubkey::new_rand();
        bank.transfer(10, &mint_keypair, &key0).unwrap();
        let orig = bank.hash_internal_state();

        // Transfer will error but still take a fee
        assert!(bank.transfer(1000, &mint_keypair, &key0).is_err());
        assert_ne!(orig, bank.hash_internal_state());

        let orig = bank.hash_internal_state();
        let empty_keypair = Keypair::new();
        assert!(bank.transfer(1000, &empty_keypair, &key0).is_err());
        assert_eq!(orig, bank.hash_internal_state());
    }

    #[test]
    fn test_bank_hash_internal_state_squash() {
        let collector_id = Pubkey::default();
        let bank0 = Arc::new(Bank::new(&create_genesis_config(10).0));
        let hash0 = bank0.hash_internal_state();
        // save hash0 because new_from_parent
        // updates sysvar entries

        let bank1 = Bank::new_from_parent(&bank0, &collector_id, 1);

        // no delta in bank1, hashes should always update
        assert_ne!(hash0, bank1.hash_internal_state());

        // remove parent
        bank1.squash();
        assert!(bank1.parents().is_empty());
    }

    #[test]
    fn test_clean_dropped_unrooted_frozen_banks() {
        solana_logger::setup();
        do_test_clean_dropped_unrooted_banks(FreezeBank1::Yes);
    }

    #[test]
    fn test_clean_dropped_unrooted_unfrozen_banks() {
        solana_logger::setup();
        do_test_clean_dropped_unrooted_banks(FreezeBank1::No);
    }

    /// A simple enum to toggle freezing Bank1 or not.  Used in the clean_dropped_unrooted tests.
    enum FreezeBank1 {
        No,
        Yes,
    }

    fn do_test_clean_dropped_unrooted_banks(freeze_bank1: FreezeBank1) {
        //! Test that dropped unrooted banks are cleaned up properly
        //!
        //! slot 0:       bank0 (rooted)
        //!               /   \
        //! slot 1:      /   bank1 (unrooted and dropped)
        //!             /
        //! slot 2:  bank2 (rooted)
        //!
        //! In the scenario above, when `clean_accounts()` is called on bank2, the keys that exist
        //! _only_ in bank1 should be cleaned up, since those keys are unreachable.
        //!
        //! The following scenarios are tested:
        //!
        //! 1. A key is written _only_ in an unrooted bank (key1)
        //!     - In this case, key1 should be cleaned up
        //! 2. A key is written in both an unrooted _and_ rooted bank (key3)
        //!     - In this case, key3's ref-count should be decremented correctly
        //! 3. A key with zero lamports is _only_ in an unrooted bank (key4)
        //!     - In this case, key4 should be cleaned up
        //! 4. A key with zero lamports is in both an unrooted _and_ rooted bank (key5)
        //!     - In this case, key5's ref-count should be decremented correctly

        let (genesis_config, mint_keypair) = create_genesis_config(100);
        let bank0 = Arc::new(Bank::new(&genesis_config));

        let collector = Pubkey::new_unique();
        let owner = Pubkey::new_unique();

        let key1 = Keypair::new(); // only touched in bank1
        let key2 = Keypair::new(); // only touched in bank2
        let key3 = Keypair::new(); // touched in both bank1 and bank2
        let key4 = Keypair::new(); // in only bank1, and has zero lamports
        let key5 = Keypair::new(); // in both bank1 and bank2, and has zero lamports

        bank0.transfer(2, &mint_keypair, &key2.pubkey()).unwrap();
        bank0.freeze();

        let slot = 1;
        let bank1 = Bank::new_from_parent(&bank0, &collector, slot);
        bank1.transfer(3, &mint_keypair, &key1.pubkey()).unwrap();
        bank1.store_account(&key4.pubkey(), &AccountSharedData::new(0, 0, &owner));
        bank1.store_account(&key5.pubkey(), &AccountSharedData::new(0, 0, &owner));

        if let FreezeBank1::Yes = freeze_bank1 {
            bank1.freeze();
        }

        let slot = slot + 1;
        let bank2 = Bank::new_from_parent(&bank0, &collector, slot);
        bank2.transfer(4, &mint_keypair, &key2.pubkey()).unwrap();
        bank2.transfer(6, &mint_keypair, &key3.pubkey()).unwrap();
        bank2.store_account(&key5.pubkey(), &AccountSharedData::new(0, 0, &owner));

        bank2.freeze(); // the freeze here is not strictly necessary, but more for illustration
        bank2.squash();

        drop(bank1);
        bank2.clean_accounts(false);

        let expected_ref_count_for_cleaned_up_keys = 0;
        let expected_ref_count_for_keys_only_in_slot_2 = bank2
            .rc
            .accounts
            .accounts_db
            .accounts_index
            .ref_count_from_storage(&key2.pubkey());

        assert_eq!(
            bank2
                .rc
                .accounts
                .accounts_db
                .accounts_index
                .ref_count_from_storage(&key1.pubkey()),
            expected_ref_count_for_cleaned_up_keys
        );
        assert_ne!(
            bank2
                .rc
                .accounts
                .accounts_db
                .accounts_index
                .ref_count_from_storage(&key3.pubkey()),
            expected_ref_count_for_cleaned_up_keys
        );
        assert_eq!(
            bank2
                .rc
                .accounts
                .accounts_db
                .accounts_index
                .ref_count_from_storage(&key4.pubkey()),
            expected_ref_count_for_cleaned_up_keys
        );
        assert_eq!(
            bank2
                .rc
                .accounts
                .accounts_db
                .accounts_index
                .ref_count_from_storage(&key5.pubkey()),
            expected_ref_count_for_keys_only_in_slot_2
        );

        assert_eq!(
            bank2.rc.accounts.accounts_db.alive_account_count_in_slot(1),
            0
        );
    }
}
