use super::{info, Bank};
use crate::builtins::ActivationType;
use feature_set::FeatureSet;
use solana_sdk::{feature, feature_set, inflation::Inflation, pubkey::Pubkey};
use std::{
    collections::HashSet,
    sync::{atomic::Ordering::Relaxed, Arc},
};

impl Bank {
    pub fn secp256k1_program_enabled(&self) -> bool {
        self.feature_set
            .is_active(&feature_set::secp256k1_program_enabled::id())
    }

    pub fn no_overflow_rent_distribution_enabled(&self) -> bool {
        self.feature_set
            .is_active(&feature_set::no_overflow_rent_distribution::id())
    }

    pub fn stake_program_v2_enabled(&self) -> bool {
        self.feature_set
            .is_active(&feature_set::stake_program_v2::id())
    }

    pub fn check_init_vote_data_enabled(&self) -> bool {
        self.feature_set
            .is_active(&feature_set::check_init_vote_data::id())
    }

    pub fn check_duplicates_by_hash_enabled(&self) -> bool {
        self.feature_set
            .is_active(&feature_set::check_duplicates_by_hash::id())
    }

    pub fn deactivate_feature(&mut self, id: &Pubkey) {
        let mut feature_set = Arc::make_mut(&mut self.feature_set).clone();
        feature_set.active.remove(&id);
        feature_set.inactive.insert(*id);
        self.feature_set = Arc::new(feature_set);
    }

    pub fn activate_feature(&mut self, id: &Pubkey) {
        let mut feature_set = Arc::make_mut(&mut self.feature_set).clone();
        feature_set.inactive.remove(id);
        feature_set.active.insert(*id, 0);
        self.feature_set = Arc::new(feature_set);
    }

    // This is called from snapshot restore AND for each epoch boundary
    // The entire code path herein must be idempotent
    pub(super) fn apply_feature_activations(&mut self, init_finish_or_warp: bool) {
        let new_feature_activations = self.compute_active_feature_set(!init_finish_or_warp);

        if new_feature_activations.contains(&feature_set::pico_inflation::id()) {
            *self.inflation.write().unwrap() = Inflation::pico();
            self.fee_rate_governor.burn_percent = 50; // 50% fee burn
            self.rent_collector.rent.burn_percent = 50; // 50% rent burn
        }

        if !new_feature_activations.is_disjoint(&self.feature_set.full_inflation_features_enabled())
        {
            *self.inflation.write().unwrap() = Inflation::full();
            self.fee_rate_governor.burn_percent = 50; // 50% fee burn
            self.rent_collector.rent.burn_percent = 50; // 50% rent burn
        }

        if new_feature_activations.contains(&feature_set::spl_token_v2_self_transfer_fix::id()) {
            self.apply_spl_token_v2_self_transfer_fix();
        }
        // Remove me after a while around v1.6
        if !self.no_stake_rewrite.load(Relaxed)
            && new_feature_activations.contains(&feature_set::rewrite_stake::id())
        {
            // to avoid any potential risk of wrongly rewriting accounts in the future,
            // only do this once, taking small risk of unknown
            // bugs which again creates bad stake accounts..

            self.rewrite_stakes();
        }

        self.ensure_feature_builtins(init_finish_or_warp, &new_feature_activations);
        self.reconfigure_token2_native_mint();
        self.ensure_no_storage_rewards_pool();
    }

    // Compute the active feature set based on the current bank state, and return the set of newly activated features
    pub(super) fn compute_active_feature_set(
        &mut self,
        allow_new_activations: bool,
    ) -> HashSet<Pubkey> {
        let mut active = self.feature_set.active.clone();
        let mut inactive = HashSet::new();
        let mut newly_activated = HashSet::new();
        let slot = self.slot();

        for feature_id in &self.feature_set.inactive {
            let mut activated = None;
            if let Some(mut account) = self.get_account_with_fixed_root(feature_id) {
                if let Some(mut feature) = feature::from_account(&account) {
                    match feature.activated_at {
                        None => {
                            if allow_new_activations {
                                // Feature has been requested, activate it now
                                feature.activated_at = Some(slot);
                                if feature::to_account(&feature, &mut account).is_some() {
                                    self.store_account(feature_id, &account);
                                }
                                newly_activated.insert(*feature_id);
                                activated = Some(slot);
                                info!("Feature {} activated at slot {}", feature_id, slot);
                            }
                        }
                        Some(activation_slot) => {
                            if slot >= activation_slot {
                                // Feature is already active
                                activated = Some(activation_slot);
                            }
                        }
                    }
                }
            }
            if let Some(slot) = activated {
                active.insert(*feature_id, slot);
            } else {
                inactive.insert(*feature_id);
            }
        }

        self.feature_set = Arc::new(FeatureSet { active, inactive });
        newly_activated
    }

    fn ensure_feature_builtins(
        &mut self,
        init_or_warp: bool,
        new_feature_activations: &HashSet<Pubkey>,
    ) {
        let feature_builtins = self.feature_builtins.clone();
        for (builtin, feature, activation_type) in feature_builtins.iter() {
            let should_populate = init_or_warp && self.feature_set.is_active(&feature)
                || !init_or_warp && new_feature_activations.contains(&feature);
            if should_populate {
                match activation_type {
                    ActivationType::NewProgram => self.add_builtin(
                        &builtin.name,
                        builtin.id,
                        builtin.process_instruction_with_context,
                    ),
                    ActivationType::NewVersion => self.replace_builtin(
                        &builtin.name,
                        builtin.id,
                        builtin.process_instruction_with_context,
                    ),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use feature::Feature;
    use solana_sdk::genesis_config::create_genesis_config;

    #[test]
    fn test_compute_active_feature_set() {
        let (genesis_config, _mint_keypair) = create_genesis_config(100_000);
        let bank0 = Arc::new(Bank::new(&genesis_config));
        let mut bank = Bank::new_from_parent(&bank0, &Pubkey::default(), 1);

        let test_feature = "TestFeature11111111111111111111111111111111"
            .parse::<Pubkey>()
            .unwrap();
        let mut feature_set = FeatureSet::default();
        feature_set.inactive.insert(test_feature);
        bank.feature_set = Arc::new(feature_set.clone());

        let new_activations = bank.compute_active_feature_set(true);
        assert!(new_activations.is_empty());
        assert!(!bank.feature_set.is_active(&test_feature));

        // Depositing into the `test_feature` account should do nothing
        bank.deposit(&test_feature, 42).unwrap();
        let new_activations = bank.compute_active_feature_set(true);
        assert!(new_activations.is_empty());
        assert!(!bank.feature_set.is_active(&test_feature));

        // Request `test_feature` activation
        let feature = Feature::default();
        assert_eq!(feature.activated_at, None);
        bank.store_account(&test_feature, &feature::create_account(&feature, 42));

        // Run `compute_active_feature_set` disallowing new activations
        let new_activations = bank.compute_active_feature_set(false);
        assert!(new_activations.is_empty());
        assert!(!bank.feature_set.is_active(&test_feature));
        let feature = feature::from_account(&bank.get_account(&test_feature).expect("get_account"))
            .expect("from_account");
        assert_eq!(feature.activated_at, None);

        // Run `compute_active_feature_set` allowing new activations
        let new_activations = bank.compute_active_feature_set(true);
        assert_eq!(new_activations.len(), 1);
        assert!(bank.feature_set.is_active(&test_feature));
        let feature = feature::from_account(&bank.get_account(&test_feature).expect("get_account"))
            .expect("from_account");
        assert_eq!(feature.activated_at, Some(1));

        // Reset the bank's feature set
        bank.feature_set = Arc::new(feature_set);
        assert!(!bank.feature_set.is_active(&test_feature));

        // Running `compute_active_feature_set` will not cause new activations, but
        // `test_feature` is now be active
        let new_activations = bank.compute_active_feature_set(true);
        assert!(new_activations.is_empty());
        assert!(bank.feature_set.is_active(&test_feature));
    }
}
