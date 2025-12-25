use xxhash_rust::xxh3::xxh3_64;

/// The number of hash functions to use.
pub const NUM_HASH_FUNCTIONS: usize = 7;
/// The number of bits per key to use for the bloom filter.
pub const BITS_PER_KEY: usize = 10;
/// The default error rate to use for the bloom filter.
pub const DEFAULT_ERROR_RATE: f64 = 0.01; // 1% error rate

/// A bloom filter is a probabilistic data structure that is used to test whether an element is a member of a set.
/// It is a bit array of size m, and a set of k hash functions.
/// The hash functions are used to hash the element to a bit in the bit array.
/// The bit array is initialized to 0.
/// When an element is added to the set, the hash functions are used to hash the element to a bit in the bit array.
/// The bit is set to 1.
/// When a query is made, the hash functions are used to hash the element to a bit in the bit array.
/// If the bit is 1, the element is a member of the set.
#[derive(Clone, Debug)]
pub struct BloomFilter {
    pub bits: Vec<u8>,
}

impl BloomFilter {
    pub fn new(num_keys: usize) -> Self{
        Self::with_bits_per_key(num_keys)
    }

    pub fn with_bits_per_key(num_keys: usize) -> Self {
        let bits_per_key = num_keys * BITS_PER_KEY;
        let bits = (bits_per_key + 7) / 8; // round up to nearest byte
        let bits = vec![0u8; bits];
        Self { bits }
    }

    /// Hash function that simulates multiple hash functions by combining the key with a seed, which returns a u64 value which is the bit index of the key.
    fn hash(&self, key: &str, seed: usize) -> u64 {
        // Hash the key once
        let h = xxh3_64(key.as_bytes());
        // Use double hashing: combine the hash with seed using a large prime multiplier
        // This ensures different seeds produce well-distributed hash values (basically h1 + seed * h2)
        h.wrapping_add(seed as u64).wrapping_mul(0x9e3779b97f4a7c15)
    }

    /// Add a key to the filter
    pub fn insert(&mut self, key: &str) {
        let num_bits = self.bits.len() * 8;
        
        for i in 0..NUM_HASH_FUNCTIONS {
            let bit_index = self.hash(key, i) % (num_bits as u64);
            let byte_index = (bit_index / 8) as usize; // get the index of the byte in the vector
            let bit_offset = (bit_index % 8) as u8; // get the offset of the bit in the byte
            self.bits[byte_index] |= 1 << bit_offset; // set the bit to 1
        }
    }

    /// Check if key might be in the set.
    /// Returns false = DEFINITELY NOT present
    /// Returns true = MAYBE present (check SSTable to confirm)
    pub fn may_contain(&self, key: &str) -> bool {
        let num_bits = self.bits.len() * 8;
        
        for i in 0..NUM_HASH_FUNCTIONS {
            let bit_index = self.hash(key, i) % (num_bits as u64);
            let byte_index = (bit_index / 8) as usize;
            let bit_offset = (bit_index % 8) as u8;
            
            if (self.bits[byte_index] & (1 << bit_offset)) == 0 {
                return false; // Bit not set = key definitely not present
            }
        }
        true // All bits set = key probably present
    }
}