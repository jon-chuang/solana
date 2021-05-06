pub struct OverwrittenVoteAccount {
    pub account: ArcVoteAccount,
    pub transaction_index: usize,
    pub transaction_result_index: usize,
}

impl Bank {
    /// current vote accounts for this bank along with the stake
    ///   attributed to each account
    /// Note: This clones the entire vote-accounts hashmap. For a single
    /// account lookup use get_vote_account instead.
    pub fn vote_accounts(&self) -> Vec<(Pubkey, (u64 /*stake*/, ArcVoteAccount))> {
        self.stakes
            .read()
            .unwrap()
            .vote_accounts()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    /// Vote account for the given vote account pubkey along with the stake.
    pub fn get_vote_account(
        &self,
        vote_account: &Pubkey,
    ) -> Option<(u64 /*stake*/, ArcVoteAccount)> {
        self.stakes
            .read()
            .unwrap()
            .vote_accounts()
            .get(vote_account)
            .cloned()
    }
}

#[test]
fn test_stake_vote_account_validity() {
    let validator_vote_keypairs0 = ValidatorVoteKeypairs::new_rand();
    let validator_vote_keypairs1 = ValidatorVoteKeypairs::new_rand();
    let validator_keypairs = vec![&validator_vote_keypairs0, &validator_vote_keypairs1];
    let GenesisConfigInfo {
        genesis_config,
        mint_keypair: _,
        voting_keypair: _,
    } = create_genesis_config_with_vote_accounts(
        1_000_000_000,
        &validator_keypairs,
        vec![10_000; 2],
    );
    let bank = Arc::new(Bank::new(&genesis_config));
    let stake_delegation_accounts = bank.stake_delegation_accounts(&mut null_tracer());
    assert_eq!(stake_delegation_accounts.len(), 2);

    let mut vote_account = bank
        .get_account(&validator_vote_keypairs0.vote_keypair.pubkey())
        .unwrap_or_default();
    let original_lamports = vote_account.lamports();
    vote_account.set_lamports(0);
    // Simulate vote account removal via full withdrawal
    bank.store_account(
        &validator_vote_keypairs0.vote_keypair.pubkey(),
        &vote_account,
    );

    // Modify staked vote account owner; a vote account owned by another program could be
    // freely modified with malicious data
    let bogus_vote_program = Pubkey::new_unique();
    vote_account.set_lamports(original_lamports);
    vote_account.set_owner(bogus_vote_program);
    bank.store_account(
        &validator_vote_keypairs0.vote_keypair.pubkey(),
        &vote_account,
    );

    assert_eq!(bank.vote_accounts().len(), 1);

    // Modify stake account owner; a stake account owned by another program could be freely
    // modified with malicious data
    let bogus_stake_program = Pubkey::new_unique();
    let mut stake_account = bank
        .get_account(&validator_vote_keypairs1.stake_keypair.pubkey())
        .unwrap_or_default();
    stake_account.set_owner(bogus_stake_program);
    bank.store_account(
        &validator_vote_keypairs1.stake_keypair.pubkey(),
        &stake_account,
    );

    // Accounts must be valid stake and vote accounts
    let stake_delegation_accounts = bank.stake_delegation_accounts(&mut null_tracer());
    assert_eq!(stake_delegation_accounts.len(), 0);
}

#[test]
fn test_vote_epoch_panic() {
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

    let vote_keypair = keypair_from_seed(&[1u8; 32]).unwrap();
    let stake_keypair = keypair_from_seed(&[2u8; 32]).unwrap();

    let mut setup_ixs = Vec::new();
    setup_ixs.extend(
        vote_instruction::create_account(
            &mint_keypair.pubkey(),
            &vote_keypair.pubkey(),
            &VoteInit {
                node_pubkey: mint_keypair.pubkey(),
                authorized_voter: vote_keypair.pubkey(),
                authorized_withdrawer: mint_keypair.pubkey(),
                commission: 0,
            },
            1_000_000_000,
        )
        .into_iter(),
    );
    setup_ixs.extend(
        stake_instruction::create_account_and_delegate_stake(
            &mint_keypair.pubkey(),
            &stake_keypair.pubkey(),
            &vote_keypair.pubkey(),
            &Authorized::auto(&mint_keypair.pubkey()),
            &Lockup::default(),
            1_000_000_000_000,
        )
        .into_iter(),
    );
    setup_ixs.push(vote_instruction::withdraw(
        &vote_keypair.pubkey(),
        &mint_keypair.pubkey(),
        1_000_000_000,
        &mint_keypair.pubkey(),
    ));
    setup_ixs.push(system_instruction::transfer(
        &mint_keypair.pubkey(),
        &vote_keypair.pubkey(),
        1_000_000_000,
    ));

    let result = bank.process_transaction(&Transaction::new(
        &[&mint_keypair, &vote_keypair, &stake_keypair],
        Message::new(&setup_ixs, Some(&mint_keypair.pubkey())),
        bank.last_blockhash(),
    ));
    assert!(result.is_ok());

    let _bank = Bank::new_from_parent(
        &bank,
        &mint_keypair.pubkey(),
        genesis_config.epoch_schedule.get_first_slot_in_epoch(1),
    );
}
