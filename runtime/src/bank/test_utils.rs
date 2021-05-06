use super::Bank;
use solana_sdk::{
    account::AccountSharedData,
    genesis_config::GenesisConfig,
    hash::Hash,
    instruction::{AccountMeta, Instruction, InstructionError},
    process_instruction::InvokeContext,
    pubkey::Pubkey,
    signature::Keypair,
    timing::years_as_slots,
    transaction::Transaction,
};
use solana_vote_program::vote_state::{BlockTimestamp, VoteState, VoteStateVersions};
use std::{result, sync::Arc};

pub(crate) fn assert_capitalization_diff(
    bank: &Bank,
    updater: impl Fn(),
    asserter: impl Fn(u64, u64),
) {
    let old = bank.capitalization();
    updater();
    let new = bank.capitalization();
    asserter(old, new);
    assert_eq!(bank.capitalization(), bank.calculate_capitalization());
}

pub(crate) fn new_from_parent(parent: &Arc<Bank>) -> Bank {
    Bank::new_from_parent(parent, &Pubkey::default(), parent.slot() + 1)
}

pub fn update_vote_account_timestamp(timestamp: BlockTimestamp, bank: &Bank, vote_pubkey: &Pubkey) {
    let mut vote_account = bank.get_account(vote_pubkey).unwrap_or_default();
    let mut vote_state = VoteState::from(&vote_account).unwrap_or_default();
    vote_state.last_timestamp = timestamp;
    let versioned = VoteStateVersions::new_current(vote_state);
    VoteState::to(&versioned, &mut vote_account).unwrap();
    bank.store_account(vote_pubkey, &vote_account);
}

fn create_child_bank_for_rent_test(
    root_bank: &Arc<Bank>,
    genesis_config: &GenesisConfig,
    mock_program_id: Pubkey,
) -> Bank {
    let mut bank = Bank::new_from_parent(
        root_bank,
        &Pubkey::default(),
        years_as_slots(
            2.0,
            &genesis_config.poh_config.target_tick_duration,
            genesis_config.ticks_per_slot,
        ) as u64,
    );
    bank.rent_collector.slots_per_year = 421_812.0;
    bank.add_builtin("mock_program", mock_program_id, mock_process_instruction);

    bank
}

#[derive(Serialize, Deserialize)]
enum MockInstruction {
    Deduction,
}

fn mock_process_instruction(
    _program_id: &Pubkey,
    data: &[u8],
    invoke_context: &mut dyn InvokeContext,
) -> result::Result<(), InstructionError> {
    let keyed_accounts = invoke_context.get_keyed_accounts()?;
    if let Ok(instruction) = bincode::deserialize(data) {
        match instruction {
            MockInstruction::Deduction => {
                keyed_accounts[1]
                    .account
                    .borrow_mut()
                    .checked_add_lamports(1)?;
                keyed_accounts[2]
                    .account
                    .borrow_mut()
                    .checked_sub_lamports(1)?;
                Ok(())
            }
        }
    } else {
        Err(InstructionError::InvalidInstructionData)
    }
}

fn create_mock_transaction(
    payer: &Keypair,
    keypair1: &Keypair,
    keypair2: &Keypair,
    read_only_keypair: &Keypair,
    mock_program_id: Pubkey,
    recent_blockhash: Hash,
) -> Transaction {
    let account_metas = vec![
        AccountMeta::new(payer.pubkey(), true),
        AccountMeta::new(keypair1.pubkey(), true),
        AccountMeta::new(keypair2.pubkey(), true),
        AccountMeta::new_readonly(read_only_keypair.pubkey(), false),
    ];
    let deduct_instruction =
        Instruction::new_with_bincode(mock_program_id, &MockInstruction::Deduction, account_metas);
    Transaction::new_signed_with_payer(
        &[deduct_instruction],
        Some(&payer.pubkey()),
        &[payer, keypair1, keypair2],
        recent_blockhash,
    )
}

fn store_accounts_for_rent_test(
    bank: &Bank,
    keypairs: &mut Vec<Keypair>,
    mock_program_id: Pubkey,
    generic_rent_due_for_system_account: u64,
) {
    let mut account_pairs: Vec<(Pubkey, AccountSharedData)> =
        Vec::with_capacity(keypairs.len() - 1);
    account_pairs.push((
        keypairs[0].pubkey(),
        AccountSharedData::new(
            generic_rent_due_for_system_account + 2,
            0,
            &Pubkey::default(),
        ),
    ));
    account_pairs.push((
        keypairs[1].pubkey(),
        AccountSharedData::new(
            generic_rent_due_for_system_account + 2,
            0,
            &Pubkey::default(),
        ),
    ));
    account_pairs.push((
        keypairs[2].pubkey(),
        AccountSharedData::new(
            generic_rent_due_for_system_account + 2,
            0,
            &Pubkey::default(),
        ),
    ));
    account_pairs.push((
        keypairs[3].pubkey(),
        AccountSharedData::new(
            generic_rent_due_for_system_account + 2,
            0,
            &Pubkey::default(),
        ),
    ));
    account_pairs.push((
        keypairs[4].pubkey(),
        AccountSharedData::new(10, 0, &Pubkey::default()),
    ));
    account_pairs.push((
        keypairs[5].pubkey(),
        AccountSharedData::new(10, 0, &Pubkey::default()),
    ));
    account_pairs.push((
        keypairs[6].pubkey(),
        AccountSharedData::new(
            (2 * generic_rent_due_for_system_account) + 24,
            0,
            &Pubkey::default(),
        ),
    ));

    account_pairs.push((
        keypairs[8].pubkey(),
        AccountSharedData::new(
            generic_rent_due_for_system_account + 2 + 929,
            0,
            &Pubkey::default(),
        ),
    ));
    account_pairs.push((
        keypairs[9].pubkey(),
        AccountSharedData::new(10, 0, &Pubkey::default()),
    ));

    // Feeding to MockProgram to test read only rent behaviour
    account_pairs.push((
        keypairs[10].pubkey(),
        AccountSharedData::new(
            generic_rent_due_for_system_account + 3,
            0,
            &Pubkey::default(),
        ),
    ));
    account_pairs.push((
        keypairs[11].pubkey(),
        AccountSharedData::new(generic_rent_due_for_system_account + 3, 0, &mock_program_id),
    ));
    account_pairs.push((
        keypairs[12].pubkey(),
        AccountSharedData::new(generic_rent_due_for_system_account + 3, 0, &mock_program_id),
    ));
    account_pairs.push((
        keypairs[13].pubkey(),
        AccountSharedData::new(14, 22, &mock_program_id),
    ));

    for account_pair in account_pairs.iter() {
        bank.store_account(&account_pair.0, &account_pair.1);
    }
}
