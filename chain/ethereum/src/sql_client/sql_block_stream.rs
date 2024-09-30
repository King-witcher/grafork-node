use crate::chain::BlockFinality;
use crate::trigger::{EthereumTrigger, LogRef};
use crate::Chain;
use graph::blockchain::block_stream::{
    BlockStream, BlockStreamError, BlockStreamEvent, BlockWithTriggers, FirehoseCursor,
};
use graph::blockchain::ToSqlFilter;
use graph::prelude::*;
use graph::{
    futures03::stream,
    futures03::stream::Stream,
    slog::{o, Logger},
};
use std::collections::VecDeque;
use std::str::FromStr;
use web3::futures::StreamExt;
use web3::types::{Bytes, Log, Transaction, H160, H256, H64, U256, U64};

use super::data::LogData;
use super::{BlockchainSqlApi, SqlClientError};

pub struct UnfoldingQueryStream {
    inner_stream:
        Pin<Box<dyn Stream<Item = Result<BlockStreamEvent<Chain>, BlockStreamError>> + Send>>,
}

struct StreamContext<Api: BlockchainSqlApi> {
    api: Api,
    logger: Logger,
    filter: Box<dyn ToSqlFilter>,
    results: VecDeque<Vec<LogData>>,
    is_last_query: bool,
}

impl<Api: BlockchainSqlApi> StreamContext<Api> {
    async fn fetch_more(&mut self) -> Result<(), SqlClientError> {
        let logs_data = self
            .api
            .execute_query_and_get_results(&self.logger, &self.filter, 1)
            .await?;
        // If the api has returned less results than the maximum, set the is_last_query flag.
        if logs_data.len() < Api::max_results() as usize {
            self.is_last_query = true;
        }

        for log_data in logs_data.into_iter() {
            // Gets a mutable reference to the last block in results
            let back_from_results = self.results.back_mut();

            // If there already is a last block
            if let Some(back_from_results) = back_from_results {
                // with the same block number as the current log_data, then push this log data into that block.
                if back_from_results[0].block_number == log_data.block_number {
                    back_from_results.push(log_data);
                } else {
                    // Otherwise if the block number is different, push a new block into ctx.results.
                    self.results.push_back(vec![log_data]);
                }
            } else {
                // If there is no blocks, push a new block with this log_data.
                self.results.push_back(vec![log_data]);
            }
        }

        Ok(())
    }
}

impl From<&LogData> for Transaction {
    fn from(value: &LogData) -> Self {
        Self {
            hash: H256::from_str(&value.tx_hash.clone().unwrap()).expect("failed to parse tx hash"), // TODO: Check
            nonce: U256::from(value.tx_nonce),
            block_hash: Some(
                H256::from_str(&value.block_hash.clone()).expect("failed to parse block hash"),
            ),
            block_number: Some(U64::from(value.block_number)),
            transaction_index: Some(U64::from(value.tx_index)),
            from: Some(
                H160::from_str(&value.tx_from.clone().unwrap()).expect("failed to parse tx from"),
            ),
            // TX to might be None when the destination address is zero.
            to: Some({
                if let Some(to) = &value.tx_to {
                    to.parse().expect("failed to parse tx to")
                } else {
                    H160::zero()
                }
            }),
            value: U256::from_str(&value.tx_value).expect("failed to parse tx value"),
            gas_price: value
                .tx_gas_price
                .map_or(None, |gas_price| Some(gas_price.into())),
            gas: value.tx_gas_used.into(),
            input: Bytes(vec![]),
            v: None,
            r: None,
            s: None,
            raw: None,
            transaction_type: None,
            access_list: None,
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
        }
    }
}

impl From<&LogData> for LightEthereumBlock {
    /// Creates a new LightEthereumBlock from a log_data, ignoraing the transaction data contained in log_data.
    fn from(log_data: &LogData) -> Self {
        Self {
            hash: Some(H256::from_str(&log_data.block_hash).expect("failed to parse block_hash")),
            parent_hash: H256::zero(),
            uncles_hash: H256::zero(),
            author: H160::zero(),
            state_root: H256::zero(),
            transactions_root: H256::zero(),
            receipts_root: H256::zero(),
            number: Some(U64::from(log_data.block_number)),
            gas_used: U256::from_str(&log_data.block_gas_used)
                .expect("failed to parse block gas used"),
            gas_limit: U256::from_str(&log_data.block_gas_limit)
                .expect("failed to parse block gas limit"),
            base_fee_per_gas: log_data
                .block_base_fee_per_gas
                .map_or(None, |value| Some(U256::from(value))),
            extra_data: Bytes(vec![]),
            logs_bloom: None,
            // timestamp: U256::from_str(&log_data.block_timestamp)
            //     .expect("failed to parse block timestamp"),
            timestamp: U256::from(0),
            difficulty: U256::from(log_data.difficulty),
            total_difficulty: log_data
                .total_difficulty
                .clone()
                .map_or(None, |value| Some(U256::from_str(&value).expect("msg"))),
            seal_fields: vec![],
            uncles: vec![],
            transactions: vec![],
            size: log_data
                .block_size
                .map_or(None, |size| Some(U256::from(size))),
            mix_hash: None,
            nonce: log_data.block_nonce.clone().map_or(None, |nonce| {
                Some(H64::from_str(&nonce).expect("failed to parse block nonce"))
            }),
        }
    }
}

impl From<&LogData> for Log {
    fn from(value: &LogData) -> Self {
        let mut topics = vec![H256::from_str(&value.topic0).expect("failed to parse topic 0")];

        if let Some(ref t1) = value.topic1 {
            let topic = H256::from_str(t1).expect("failed to parse topic 1");
            topics.push(topic);
        }

        if let Some(ref t2) = value.topic2 {
            let topic = H256::from_str(t2).expect("failed to parse topic 2");
            topics.push(topic);
        }

        if let Some(ref t3) = value.topic3 {
            let topic = H256::from_str(t3).expect("failed to parse topic 3");
            topics.push(topic);
        }

        Log {
            address: H160::from_str(&value.log_address).expect("failed to parse H160"),
            topics,
            data: serde_json::from_str(&format!("\"{}\"", value.log_data)).unwrap(), // ?
            block_hash: Some(
                H256::from_str(&value.block_hash).expect("failed to parse block_hash"),
            ),
            block_number: Some(U64::from(value.block_number)),
            transaction_hash: value.tx_hash.clone().map_or(None, |hash| {
                Some(H256::from_str(&hash).expect("failed to parse tx hash"))
            }),
            transaction_index: Some(U64::from(value.tx_index)),
            log_index: Some(U256::from(value.log_index)),
            transaction_log_index: None,
            log_type: None,
            removed: Some(false),
        }
    }
}

fn create_log_ethereum_trigger(log: Log) -> EthereumTrigger {
    EthereumTrigger::Log(LogRef::FullLog(Arc::new(log), None))
}

/// Creates an Ethereum BlockStreamEvent<C> with an iterator of log_data.
///
/// **All log_data entries must have the same block number.**
fn create_block_stream_event(
    mut log_data_iter: impl Iterator<Item = LogData>,
) -> BlockStreamEvent<Chain> {
    let first = log_data_iter
        .next()
        .expect("A logs iterator cannot be empty");

    let mut block = LightEthereumBlock::from(&first);
    let mut transactions = vec![Transaction::from(&first)];
    let mut triggers = vec![create_log_ethereum_trigger(Log::from(&first))];

    // Keeps track of the lat tx index to decide whether or not to create a new transaction and push to the array
    let mut last_tx_index = first.tx_index;
    for log_data in log_data_iter {
        triggers.push(create_log_ethereum_trigger(Log::from(&log_data)));

        // If a different transaction was found, add to transactions array and update last transaction index.
        if log_data.tx_index != last_tx_index {
            transactions.push(Transaction::from(&log_data));
            last_tx_index = log_data.tx_index;
        }
    }

    block.transactions = transactions;

    BlockStreamEvent::<Chain>::ProcessBlock(
        BlockWithTriggers::<Chain> {
            block: BlockFinality::Final(Arc::new(block)),
            trigger_data: triggers,
        },
        FirehoseCursor::None,
    )
}

impl UnfoldingQueryStream {
    pub fn from_sql_api<Api: BlockchainSqlApi>(
        logger: &Logger,
        filter: Box<dyn ToSqlFilter>,
        api: Api,
    ) -> Self {
        let ctx = StreamContext {
            api,
            results: vec![].into(),
            is_last_query: false,
            filter,
            logger: logger.new(o!("component" => "UnfoldingQueryStream")),
        };

        let inner_stream = stream::unfold(ctx, Self::get_next);

        return Self {
            inner_stream: Box::pin(inner_stream),
        };
    }

    async fn get_next<Api: BlockchainSqlApi>(
        mut ctx: StreamContext<Api>,
    ) -> Option<(
        Result<BlockStreamEvent<Chain>, BlockStreamError>,
        StreamContext<Api>,
    )> {
        // If there is one or less blocks, make another query that might complete the current last block.
        if ctx.results.len() <= 1 && !ctx.is_last_query {
            match ctx.fetch_more().await {
                Ok(_) => {}
                Err(error) => {
                    crit!(
                        ctx.logger,
                        "failed to query for logs due to {error}. Aborting"
                    );
                    return None;
                }
            };
        }

        if ctx.results.len() != 0 {
            let last = ctx.results.pop_front();
            if last.is_some() {
                return Some((
                    Ok(create_block_stream_event(last.unwrap().into_iter())),
                    ctx,
                ));
            } else {
                return None;
            }
        }

        None
    }
}

impl BlockStream<Chain> for UnfoldingQueryStream {
    fn buffer_size_hint(&self) -> usize {
        1
    }
}

impl Stream for UnfoldingQueryStream {
    type Item = Result<BlockStreamEvent<Chain>, BlockStreamError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner_stream.poll_next_unpin(cx)
    }
}
