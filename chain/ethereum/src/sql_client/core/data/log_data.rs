use graph::prelude::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct LogData {
    // Block
    pub block_number: i32,
    pub block_hash: String,
    pub parent_hash: String,
    pub difficulty: i64,
    pub total_difficulty: Option<String>,
    pub block_gas_limit: String,
    pub block_gas_used: String,
    pub block_base_fee_per_gas: Option<i64>,
    pub miner: String,
    pub block_nonce: Option<String>,
    pub block_size: Option<i64>,
    pub block_timestamp: String,

    // Transaction
    pub tx_index: Option<i64>,
    pub tx_hash: Option<String>,
    pub tx_gas_used: i64,
    pub tx_from: Option<String>,
    pub tx_to: Option<String>,
    pub tx_type: Option<String>,
    // pub tx_success: bool,
    pub tx_gas_price: Option<i64>,
    pub tx_gas_limit: i64,
    pub max_priority_fee_per_gas: Option<i64>,
    pub max_fee_per_gas: Option<i64>,
    pub tx_nonce: i64,
    pub tx_value: String,

    // Log
    pub log_index: i32,
    pub log_address: String,
    pub topic0: String,
    pub topic1: Option<String>,
    pub topic2: Option<String>,
    pub topic3: Option<String>,
    pub log_data: String,
}
