use super::Bank;
use solana_sdk::{
    account::from_account,
    clock::{Epoch, SlotCount, DEFAULT_TICKS_PER_SECOND, SECONDS_PER_DAY},
    sysvar,
};
use std::sync::atomic::Ordering::Relaxed;
impl Bank {
    /// computed unix_timestamp at this slot height
    pub fn unix_timestamp_from_genesis(&self) -> i64 {
        self.genesis_creation_time + ((self.slot as u128 * self.ns_per_slot) / 1_000_000_000) as i64
    }

    pub fn epoch_duration_in_years(&self, prev_epoch: Epoch) -> f64 {
        // period: time that has passed as a fraction of a year, basically the length of
        //  an epoch as a fraction of a year
        //  calculated as: slots_elapsed / (slots / year)
        self.epoch_schedule.get_slots_in_epoch(prev_epoch) as f64 / self.slots_per_year
    }

    pub fn clock(&self) -> sysvar::clock::Clock {
        from_account(&self.get_account(&sysvar::clock::id()).unwrap_or_default())
            .unwrap_or_default()
    }

    /// Return the number of hashes per tick
    pub fn hashes_per_tick(&self) -> &Option<u64> {
        &self.hashes_per_tick
    }

    /// Return the number of ticks per slot
    pub fn ticks_per_slot(&self) -> u64 {
        self.ticks_per_slot
    }

    /// Return the number of slots per year
    pub fn slots_per_year(&self) -> f64 {
        self.slots_per_year
    }

    /// Return the number of ticks since genesis.
    pub fn tick_height(&self) -> u64 {
        self.tick_height.load(Relaxed)
    }

    /// Return this bank's max_tick_height
    pub fn max_tick_height(&self) -> u64 {
        self.max_tick_height
    }

    /// Return the block_height of this bank
    pub fn block_height(&self) -> u64 {
        self.block_height
    }

    /// Return the number of slots per epoch for the given epoch
    pub fn get_slots_in_epoch(&self, epoch: Epoch) -> u64 {
        self.epoch_schedule.get_slots_in_epoch(epoch)
    }

    // This value is specially chosen to align with slots per epoch in mainnet-beta and testnet
    // Also, assume 500GB account data set as the extreme, then for 2 day (=48 hours) to collect
    // rent eagerly, we'll consume 5.7 MB/s IO bandwidth, bidirectionally.
    fn slot_count_in_two_day(&self) -> SlotCount {
        2 * DEFAULT_TICKS_PER_SECOND * SECONDS_PER_DAY / self.ticks_per_slot
    }

    fn slot_count_per_normal_epoch(&self) -> SlotCount {
        self.get_slots_in_epoch(self.first_normal_epoch())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        bank::test_utils::{new_from_parent, update_vote_account_timestamp},
        genesis_utils::{create_genesis_config_with_leader, GenesisConfigInfo},
    };
    use solana_sdk::{
        genesis_config::create_genesis_config, signature::Signer, timing::duration_as_s,
    };
    use solana_vote_program::vote_state::BlockTimestamp;
    use std::sync::Arc;

    #[test]
    fn test_bank_unix_timestamp_from_genesis() {
        let (genesis_config, _mint_keypair) = create_genesis_config(1);
        let mut bank = Arc::new(Bank::new(&genesis_config));

        assert_eq!(
            genesis_config.creation_time,
            bank.unix_timestamp_from_genesis()
        );
        let slots_per_sec = 1.0
            / (duration_as_s(&genesis_config.poh_config.target_tick_duration)
                * genesis_config.ticks_per_slot as f32);

        for _i in 0..slots_per_sec as usize + 1 {
            bank = Arc::new(new_from_parent(&bank));
        }

        assert!(bank.unix_timestamp_from_genesis() - genesis_config.creation_time >= 1);
    }

    #[test]
    fn test_update_clock_timestamp() {
        let leader_pubkey = solana_sdk::pubkey::new_rand();
        let GenesisConfigInfo {
            genesis_config,
            voting_keypair,
            ..
        } = create_genesis_config_with_leader(5, &leader_pubkey, 3);
        let mut bank = Bank::new(&genesis_config);
        // Advance past slot 0, which has special handling.
        bank = new_from_parent(&Arc::new(bank));
        bank = new_from_parent(&Arc::new(bank));
        assert_eq!(
            bank.clock().unix_timestamp,
            bank.unix_timestamp_from_genesis()
        );

        bank.update_clock(None);
        assert_eq!(
            bank.clock().unix_timestamp,
            bank.unix_timestamp_from_genesis()
        );

        update_vote_account_timestamp(
            BlockTimestamp {
                slot: bank.slot(),
                timestamp: bank.unix_timestamp_from_genesis() - 1,
            },
            &bank,
            &voting_keypair.pubkey(),
        );
        bank.update_clock(None);
        assert_eq!(
            bank.clock().unix_timestamp,
            bank.unix_timestamp_from_genesis()
        );

        update_vote_account_timestamp(
            BlockTimestamp {
                slot: bank.slot(),
                timestamp: bank.unix_timestamp_from_genesis(),
            },
            &bank,
            &voting_keypair.pubkey(),
        );
        bank.update_clock(None);
        assert_eq!(
            bank.clock().unix_timestamp,
            bank.unix_timestamp_from_genesis()
        );

        update_vote_account_timestamp(
            BlockTimestamp {
                slot: bank.slot(),
                timestamp: bank.unix_timestamp_from_genesis() + 1,
            },
            &bank,
            &voting_keypair.pubkey(),
        );
        bank.update_clock(None);
        assert_eq!(
            bank.clock().unix_timestamp,
            bank.unix_timestamp_from_genesis() + 1
        );

        // Timestamp cannot go backward from ancestor Bank to child
        bank = new_from_parent(&Arc::new(bank));
        update_vote_account_timestamp(
            BlockTimestamp {
                slot: bank.slot(),
                timestamp: bank.unix_timestamp_from_genesis() - 1,
            },
            &bank,
            &voting_keypair.pubkey(),
        );
        bank.update_clock(None);
        assert_eq!(
            bank.clock().unix_timestamp,
            bank.unix_timestamp_from_genesis()
        );
    }

    #[test]
    fn test_bank_get_slots_in_epoch() {
        let (genesis_config, _) = create_genesis_config(500);

        let bank = Bank::new(&genesis_config);

        assert_eq!(bank.get_slots_in_epoch(0), MINIMUM_SLOTS_PER_EPOCH as u64);
        assert_eq!(
            bank.get_slots_in_epoch(2),
            (MINIMUM_SLOTS_PER_EPOCH * 4) as u64
        );
        assert_eq!(
            bank.get_slots_in_epoch(5000),
            genesis_config.epoch_schedule.slots_per_epoch
        );
    }
}
