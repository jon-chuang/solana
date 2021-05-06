#[cfg(test)]
pub(crate) mod tests {

    #[test]
    fn test_get_inflation_start_slot_devnet_testnet() {
        let GenesisConfigInfo {
            mut genesis_config, ..
        } = create_genesis_config_with_leader(42, &solana_sdk::pubkey::new_rand(), 42);
        genesis_config
            .accounts
            .remove(&feature_set::pico_inflation::id())
            .unwrap();
        genesis_config
            .accounts
            .remove(&feature_set::full_inflation::devnet_and_testnet::id())
            .unwrap();
        for pair in feature_set::FULL_INFLATION_FEATURE_PAIRS.iter() {
            genesis_config.accounts.remove(&pair.vote_id).unwrap();
            genesis_config.accounts.remove(&pair.enable_id).unwrap();
        }

        let bank = Bank::new(&genesis_config);

        // Advance slot
        let mut bank = new_from_parent(&Arc::new(bank));
        bank = new_from_parent(&Arc::new(bank));
        assert_eq!(bank.get_inflation_start_slot(), 0);
        assert_eq!(bank.slot(), 2);

        // Request `pico_inflation` activation
        bank.store_account(
            &feature_set::pico_inflation::id(),
            &feature::create_account(
                &Feature {
                    activated_at: Some(1),
                },
                42,
            ),
        );
        bank.compute_active_feature_set(true);
        assert_eq!(bank.get_inflation_start_slot(), 1);

        // Advance slot
        bank = new_from_parent(&Arc::new(bank));
        assert_eq!(bank.slot(), 3);

        // Request `full_inflation::devnet_and_testnet` activation,
        // which takes priority over pico_inflation
        bank.store_account(
            &feature_set::full_inflation::devnet_and_testnet::id(),
            &feature::create_account(
                &Feature {
                    activated_at: Some(2),
                },
                42,
            ),
        );
        bank.compute_active_feature_set(true);
        assert_eq!(bank.get_inflation_start_slot(), 2);

        // Request `full_inflation::mainnet::certusone` activation,
        // which should have no effect on `get_inflation_start_slot`
        bank.store_account(
            &feature_set::full_inflation::mainnet::certusone::vote::id(),
            &feature::create_account(
                &Feature {
                    activated_at: Some(3),
                },
                42,
            ),
        );
        bank.store_account(
            &feature_set::full_inflation::mainnet::certusone::enable::id(),
            &feature::create_account(
                &Feature {
                    activated_at: Some(3),
                },
                42,
            ),
        );
        bank.compute_active_feature_set(true);
        assert_eq!(bank.get_inflation_start_slot(), 2);
    }

    #[test]
    fn test_get_inflation_start_slot_mainnet() {
        let GenesisConfigInfo {
            mut genesis_config, ..
        } = create_genesis_config_with_leader(42, &solana_sdk::pubkey::new_rand(), 42);
        genesis_config
            .accounts
            .remove(&feature_set::pico_inflation::id())
            .unwrap();
        genesis_config
            .accounts
            .remove(&feature_set::full_inflation::devnet_and_testnet::id())
            .unwrap();
        for pair in feature_set::FULL_INFLATION_FEATURE_PAIRS.iter() {
            genesis_config.accounts.remove(&pair.vote_id).unwrap();
            genesis_config.accounts.remove(&pair.enable_id).unwrap();
        }

        let bank = Bank::new(&genesis_config);

        // Advance slot
        let mut bank = new_from_parent(&Arc::new(bank));
        bank = new_from_parent(&Arc::new(bank));
        assert_eq!(bank.get_inflation_start_slot(), 0);
        assert_eq!(bank.slot(), 2);

        // Request `pico_inflation` activation
        bank.store_account(
            &feature_set::pico_inflation::id(),
            &feature::create_account(
                &Feature {
                    activated_at: Some(1),
                },
                42,
            ),
        );
        bank.compute_active_feature_set(true);
        assert_eq!(bank.get_inflation_start_slot(), 1);

        // Advance slot
        bank = new_from_parent(&Arc::new(bank));
        assert_eq!(bank.slot(), 3);

        // Request `full_inflation::mainnet::certusone` activation,
        // which takes priority over pico_inflation
        bank.store_account(
            &feature_set::full_inflation::mainnet::certusone::vote::id(),
            &feature::create_account(
                &Feature {
                    activated_at: Some(2),
                },
                42,
            ),
        );
        bank.store_account(
            &feature_set::full_inflation::mainnet::certusone::enable::id(),
            &feature::create_account(
                &Feature {
                    activated_at: Some(2),
                },
                42,
            ),
        );
        bank.compute_active_feature_set(true);
        assert_eq!(bank.get_inflation_start_slot(), 2);

        // Advance slot
        bank = new_from_parent(&Arc::new(bank));
        assert_eq!(bank.slot(), 4);

        // Request `full_inflation::devnet_and_testnet` activation,
        // which should have no effect on `get_inflation_start_slot`
        bank.store_account(
            &feature_set::full_inflation::devnet_and_testnet::id(),
            &feature::create_account(
                &Feature {
                    activated_at: Some(bank.slot()),
                },
                42,
            ),
        );
        bank.compute_active_feature_set(true);
        assert_eq!(bank.get_inflation_start_slot(), 2);
    }

    #[test]
    fn test_get_inflation_num_slots_with_activations() {
        let GenesisConfigInfo {
            mut genesis_config, ..
        } = create_genesis_config_with_leader(42, &solana_sdk::pubkey::new_rand(), 42);
        let slots_per_epoch = 32;
        genesis_config.epoch_schedule = EpochSchedule::new(slots_per_epoch);
        genesis_config
            .accounts
            .remove(&feature_set::pico_inflation::id())
            .unwrap();
        genesis_config
            .accounts
            .remove(&feature_set::full_inflation::devnet_and_testnet::id())
            .unwrap();
        for pair in feature_set::FULL_INFLATION_FEATURE_PAIRS.iter() {
            genesis_config.accounts.remove(&pair.vote_id).unwrap();
            genesis_config.accounts.remove(&pair.enable_id).unwrap();
        }

        let mut bank = Bank::new(&genesis_config);
        assert_eq!(bank.get_inflation_num_slots(), 0);
        for _ in 0..2 * slots_per_epoch {
            bank = new_from_parent(&Arc::new(bank));
        }
        assert_eq!(bank.get_inflation_num_slots(), 2 * slots_per_epoch);

        // Activate pico_inflation
        let pico_inflation_activation_slot = bank.slot();
        bank.store_account(
            &feature_set::pico_inflation::id(),
            &feature::create_account(
                &Feature {
                    activated_at: Some(pico_inflation_activation_slot),
                },
                42,
            ),
        );
        bank.compute_active_feature_set(true);
        assert_eq!(bank.get_inflation_num_slots(), slots_per_epoch);
        for _ in 0..slots_per_epoch {
            bank = new_from_parent(&Arc::new(bank));
        }
        assert_eq!(bank.get_inflation_num_slots(), 2 * slots_per_epoch);

        // Activate full_inflation::devnet_and_testnet
        let full_inflation_activation_slot = bank.slot();
        bank.store_account(
            &feature_set::full_inflation::devnet_and_testnet::id(),
            &feature::create_account(
                &Feature {
                    activated_at: Some(full_inflation_activation_slot),
                },
                42,
            ),
        );
        bank.compute_active_feature_set(true);
        assert_eq!(bank.get_inflation_num_slots(), slots_per_epoch);
        for _ in 0..slots_per_epoch {
            bank = new_from_parent(&Arc::new(bank));
        }
        assert_eq!(bank.get_inflation_num_slots(), 2 * slots_per_epoch);
    }

    #[test]
    fn test_get_inflation_num_slots_already_activated() {
        let GenesisConfigInfo {
            mut genesis_config, ..
        } = create_genesis_config_with_leader(42, &solana_sdk::pubkey::new_rand(), 42);
        let slots_per_epoch = 32;
        genesis_config.epoch_schedule = EpochSchedule::new(slots_per_epoch);
        let mut bank = Bank::new(&genesis_config);
        assert_eq!(bank.get_inflation_num_slots(), 0);
        for _ in 0..slots_per_epoch {
            bank = new_from_parent(&Arc::new(bank));
        }
        assert_eq!(bank.get_inflation_num_slots(), slots_per_epoch);
        for _ in 0..slots_per_epoch {
            bank = new_from_parent(&Arc::new(bank));
        }
        assert_eq!(bank.get_inflation_num_slots(), 2 * slots_per_epoch);
    }
}
