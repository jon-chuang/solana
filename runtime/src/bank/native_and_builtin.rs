use super::{debug, Bank};
use crate::builtins::ActivationType;
use solana_sdk::{
    account::{ReadableAccount, WritableAccount},
    native_loader,
    process_instruction::ProcessInstructionWithContext,
    pubkey::Pubkey,
};
use std::{fmt, sync::atomic::Ordering::Relaxed};

#[derive(Clone)]
pub struct Builtin {
    pub name: String,
    pub id: Pubkey,
    pub process_instruction_with_context: ProcessInstructionWithContext,
}

impl Builtin {
    pub fn new(
        name: &str,
        id: Pubkey,
        process_instruction_with_context: ProcessInstructionWithContext,
    ) -> Self {
        Self {
            name: name.to_string(),
            id,
            process_instruction_with_context,
        }
    }
}

impl fmt::Debug for Builtin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Builtin [name={}, id={}]", self.name, self.id)
    }
}

#[cfg(RUSTC_WITH_SPECIALIZATION)]
impl AbiExample for Builtin {
    fn example() -> Self {
        Self {
            name: String::default(),
            id: Pubkey::default(),
            process_instruction_with_context: |_, _, _| Ok(()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Builtins {
    /// Builtin programs that are always available
    pub genesis_builtins: Vec<Builtin>,

    /// Builtin programs activated dynamically by feature
    pub feature_builtins: Vec<(Builtin, Pubkey, ActivationType)>,
}

impl Bank {
    /// Add an instruction processor to intercept instructions before the dynamic loader.
    pub fn add_builtin(
        &mut self,
        name: &str,
        program_id: Pubkey,
        process_instruction_with_context: ProcessInstructionWithContext,
    ) {
        debug!("Adding program {} under {:?}", name, program_id);
        self.add_native_program(name, &program_id, false);
        self.message_processor
            .add_program(program_id, process_instruction_with_context);
    }

    /// Replace a builtin instruction processor if it already exists
    pub fn replace_builtin(
        &mut self,
        name: &str,
        program_id: Pubkey,
        process_instruction_with_context: ProcessInstructionWithContext,
    ) {
        debug!("Replacing program {} under {:?}", name, program_id);
        self.add_native_program(name, &program_id, true);
        self.message_processor
            .add_program(program_id, process_instruction_with_context);
    }

    // NOTE: must hold idempotent for the same set of arguments
    pub fn add_native_program(&self, name: &str, program_id: &Pubkey, must_replace: bool) {
        let existing_genuine_program =
            if let Some(mut account) = self.get_account_with_fixed_root(&program_id) {
                // it's very unlikely to be squatted at program_id as non-system account because of burden to
                // find victim's pubkey/hash. So, when account.owner is indeed native_loader's, it's
                // safe to assume it's a genuine program.
                if native_loader::check_id(&account.owner()) {
                    Some(account)
                } else {
                    // malicious account is pre-occupying at program_id
                    // forcibly burn and purge it

                    self.capitalization.fetch_sub(account.lamports(), Relaxed);

                    // Resetting account balance to 0 is needed to really purge from AccountsDb and
                    // flush the Stakes cache
                    account.set_lamports(0);
                    self.store_account(&program_id, &account);
                    None
                }
            } else {
                None
            };

        if must_replace {
            // updating native program

            match &existing_genuine_program {
                None => panic!(
                    "There is no account to replace with native program ({}, {}).",
                    name, program_id
                ),
                Some(account) => {
                    if *name == String::from_utf8_lossy(&account.data()) {
                        // nop; it seems that already AccountsDb is updated.
                        return;
                    }
                    // continue to replace account
                }
            }
        } else {
            // introducing native program

            match &existing_genuine_program {
                None => (), // continue to add account
                Some(_account) => {
                    // nop; it seems that we already have account

                    // before returning here to retain idempotent just make sure
                    // the existing native program name is same with what we're
                    // supposed to add here (but skipping) But I can't:
                    // following assertion already catches several different names for same
                    // program_id
                    // depending on clusters...
                    // assert_eq!(name.to_owned(), String::from_utf8_lossy(&account.data));
                    return;
                }
            }
        }

        assert!(
            !self.freeze_started(),
            "Can't change frozen bank by adding not-existing new native program ({}, {}). \
                Maybe, inconsistent program activation is detected on snapshot restore?",
            name,
            program_id
        );

        // Add a bogus executable native account, which will be loaded and ignored.
        let account = native_loader::create_loadable_account_with_fields(
            name,
            self.inherit_specially_retained_account_fields(&existing_genuine_program),
        );
        self.store_account_and_update_capitalization(&program_id, &account);

        debug!("Added native program {} under {:?}", name, program_id);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        bank::test_utils::{assert_capitalization_diff, new_from_parent},
        bank::Bank,
        genesis_utils::activate_all_features,
    };
    use solana_sdk::{
        genesis_config::create_genesis_config,
        instruction::{AccountMeta, Instruction, InstructionError, NativeLoaderError},
        message::Message,
        native_loader,
        process_instruction::InvokeContext,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        system_instruction,
        transaction::{Transaction, TransactionError},
    };
    use solana_vote_program::{vote_instruction, vote_state::VoteInit};
    use std::sync::Arc;

    #[test]
    fn test_add_builtin() {
        let (genesis_config, mint_keypair) = create_genesis_config(500);
        let mut bank = Bank::new(&genesis_config);

        fn mock_vote_program_id() -> Pubkey {
            Pubkey::new(&[42u8; 32])
        }
        fn mock_vote_processor(
            program_id: &Pubkey,
            _instruction_data: &[u8],
            _invoke_context: &mut dyn InvokeContext,
        ) -> std::result::Result<(), InstructionError> {
            if mock_vote_program_id() != *program_id {
                return Err(InstructionError::IncorrectProgramId);
            }
            Err(InstructionError::Custom(42))
        }

        assert!(bank.get_account(&mock_vote_program_id()).is_none());
        bank.add_builtin(
            "mock_vote_program",
            mock_vote_program_id(),
            mock_vote_processor,
        );
        assert!(bank.get_account(&mock_vote_program_id()).is_some());

        let mock_account = Keypair::new();
        let mock_validator_identity = Keypair::new();
        let mut instructions = vote_instruction::create_account(
            &mint_keypair.pubkey(),
            &mock_account.pubkey(),
            &VoteInit {
                node_pubkey: mock_validator_identity.pubkey(),
                ..VoteInit::default()
            },
            1,
        );
        instructions[1].program_id = mock_vote_program_id();

        let message = Message::new(&instructions, Some(&mint_keypair.pubkey()));
        let transaction = Transaction::new(
            &[&mint_keypair, &mock_account, &mock_validator_identity],
            message,
            bank.last_blockhash(),
        );

        assert_eq!(
            bank.process_transaction(&transaction),
            Err(TransactionError::InstructionError(
                1,
                InstructionError::Custom(42)
            ))
        );
    }

    #[test]
    fn test_add_native_program() {
        let (mut genesis_config, _mint_keypair) = create_genesis_config(100_000);
        activate_all_features(&mut genesis_config);

        let slot = 123;
        let program_id = solana_sdk::pubkey::new_rand();

        let bank = Arc::new(Bank::new_from_parent(
            &Arc::new(Bank::new(&genesis_config)),
            &Pubkey::default(),
            slot,
        ));
        assert_eq!(bank.get_account_modified_slot(&program_id), None);

        assert_capitalization_diff(
            &bank,
            || bank.add_native_program("mock_program", &program_id, false),
            |old, new| {
                assert_eq!(old + 1, new);
            },
        );

        assert_eq!(bank.get_account_modified_slot(&program_id).unwrap().1, slot);

        let bank = Arc::new(new_from_parent(&bank));
        assert_capitalization_diff(
            &bank,
            || bank.add_native_program("mock_program", &program_id, false),
            |old, new| assert_eq!(old, new),
        );

        assert_eq!(bank.get_account_modified_slot(&program_id).unwrap().1, slot);

        let bank = Arc::new(new_from_parent(&bank));
        // When replacing native_program, name must change to disambiguate from repeated
        // invocations.
        assert_capitalization_diff(
            &bank,
            || bank.add_native_program("mock_program v2", &program_id, true),
            |old, new| assert_eq!(old, new),
        );

        assert_eq!(
            bank.get_account_modified_slot(&program_id).unwrap().1,
            bank.slot()
        );

        let bank = Arc::new(new_from_parent(&bank));
        assert_capitalization_diff(
            &bank,
            || bank.add_native_program("mock_program v2", &program_id, true),
            |old, new| assert_eq!(old, new),
        );

        // replacing with same name shouldn't update account
        assert_eq!(
            bank.get_account_modified_slot(&program_id).unwrap().1,
            bank.parent_slot()
        );
    }

    #[test]
    fn test_add_native_program_inherited_cap_while_replacing() {
        let (genesis_config, mint_keypair) = create_genesis_config(100_000);
        let bank = Bank::new(&genesis_config);
        let program_id = solana_sdk::pubkey::new_rand();

        bank.add_native_program("mock_program", &program_id, false);
        assert_eq!(bank.capitalization(), bank.calculate_capitalization());

        // someone mess with program_id's balance
        bank.withdraw(&mint_keypair.pubkey(), 10).unwrap();
        assert_ne!(bank.capitalization(), bank.calculate_capitalization());
        bank.deposit(&program_id, 10).unwrap();
        assert_eq!(bank.capitalization(), bank.calculate_capitalization());

        bank.add_native_program("mock_program v2", &program_id, true);
        assert_eq!(bank.capitalization(), bank.calculate_capitalization());
    }

    #[test]
    fn test_add_native_program_squatted_while_not_replacing() {
        let (genesis_config, mint_keypair) = create_genesis_config(100_000);
        let bank = Bank::new(&genesis_config);
        let program_id = solana_sdk::pubkey::new_rand();

        // someone managed to squat at program_id!
        bank.withdraw(&mint_keypair.pubkey(), 10).unwrap();
        assert_ne!(bank.capitalization(), bank.calculate_capitalization());
        bank.deposit(&program_id, 10).unwrap();
        assert_eq!(bank.capitalization(), bank.calculate_capitalization());

        bank.add_native_program("mock_program", &program_id, false);
        assert_eq!(bank.capitalization(), bank.calculate_capitalization());
    }

    #[test]
    #[should_panic(
        expected = "Can't change frozen bank by adding not-existing new native \
                       program (mock_program, CiXgo2KHKSDmDnV1F6B69eWFgNAPiSBjjYvfB4cvRNre). \
                       Maybe, inconsistent program activation is detected on snapshot restore?"
    )]
    fn test_add_native_program_after_frozen() {
        use std::str::FromStr;
        let (genesis_config, _mint_keypair) = create_genesis_config(100_000);

        let slot = 123;
        let program_id = Pubkey::from_str("CiXgo2KHKSDmDnV1F6B69eWFgNAPiSBjjYvfB4cvRNre").unwrap();

        let bank = Bank::new_from_parent(
            &Arc::new(Bank::new(&genesis_config)),
            &Pubkey::default(),
            slot,
        );
        bank.freeze();

        bank.add_native_program("mock_program", &program_id, false);
    }

    #[test]
    #[should_panic(
        expected = "There is no account to replace with native program (mock_program, \
                    CiXgo2KHKSDmDnV1F6B69eWFgNAPiSBjjYvfB4cvRNre)."
    )]
    fn test_add_native_program_replace_none() {
        use std::str::FromStr;
        let (genesis_config, _mint_keypair) = create_genesis_config(100_000);

        let slot = 123;
        let program_id = Pubkey::from_str("CiXgo2KHKSDmDnV1F6B69eWFgNAPiSBjjYvfB4cvRNre").unwrap();

        let bank = Bank::new_from_parent(
            &Arc::new(Bank::new(&genesis_config)),
            &Pubkey::default(),
            slot,
        );

        bank.add_native_program("mock_program", &program_id, true);
    }

    #[test]
    fn test_program_is_native_loader() {
        let (genesis_config, mint_keypair) = create_genesis_config(50000);
        let bank = Bank::new(&genesis_config);

        let tx = Transaction::new_signed_with_payer(
            &[Instruction::new_with_bincode(
                native_loader::id(),
                &(),
                vec![],
            )],
            Some(&mint_keypair.pubkey()),
            &[&mint_keypair],
            bank.last_blockhash(),
        );
        assert_eq!(
            bank.process_transaction(&tx),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::UnsupportedProgramId
            ))
        );
    }

    #[test]
    fn test_bad_native_loader() {
        let (genesis_config, mint_keypair) = create_genesis_config(50000);
        let bank = Bank::new(&genesis_config);
        let to_keypair = Keypair::new();

        let tx = Transaction::new_signed_with_payer(
            &[
                system_instruction::create_account(
                    &mint_keypair.pubkey(),
                    &to_keypair.pubkey(),
                    10000,
                    0,
                    &native_loader::id(),
                ),
                Instruction::new_with_bincode(
                    native_loader::id(),
                    &(),
                    vec![AccountMeta::new(to_keypair.pubkey(), false)],
                ),
            ],
            Some(&mint_keypair.pubkey()),
            &[&mint_keypair, &to_keypair],
            bank.last_blockhash(),
        );
        assert_eq!(
            bank.process_transaction(&tx),
            Err(TransactionError::InstructionError(
                1,
                InstructionError::Custom(NativeLoaderError::InvalidAccountData as u32)
            ))
        );

        let tx = Transaction::new_signed_with_payer(
            &[
                system_instruction::create_account(
                    &mint_keypair.pubkey(),
                    &to_keypair.pubkey(),
                    10000,
                    100,
                    &native_loader::id(),
                ),
                Instruction::new_with_bincode(
                    native_loader::id(),
                    &(),
                    vec![AccountMeta::new(to_keypair.pubkey(), false)],
                ),
            ],
            Some(&mint_keypair.pubkey()),
            &[&mint_keypair, &to_keypair],
            bank.last_blockhash(),
        );
        assert_eq!(
            bank.process_transaction(&tx),
            Err(TransactionError::InstructionError(
                1,
                InstructionError::Custom(NativeLoaderError::InvalidAccountData as u32)
            ))
        );
    }
}
