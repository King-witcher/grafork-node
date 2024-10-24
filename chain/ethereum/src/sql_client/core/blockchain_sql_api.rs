use graph::prelude::*;
use serde::Deserialize;

use crate::sql_client::core::data::LogData;
use graph::blockchain::SqlFilterWithCursor;
use std;
use std::cmp::PartialEq;
use std::future::Future;
use std::time::{Duration, Instant};
use tokio;
use tokio::time;

use crate::sql_client::core::SqlClientError;
use crate::sql_client::SqlClientResult;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QueryExecutionStatus {
    /// The query is pending and did not start yet.
    Pending,

    /// The query is being executed.
    Executing,

    /// The query is ready.
    Completed { size: i64 },

    /// The query failed for some reason.
    Failed,

    /// The query has been cancelled.
    Cancelled,

    /// The query has been expired before it could complete.
    Expired,
}

/// Represents a range where a query can be run with max results.
#[derive(Debug, Deserialize)]
pub struct BlockRange {
    start_block_number: String,
    start_log_index: String,
    end_block_number: String,
    end_log_index: String,
    logs_count: u64,
}

/// Represents an adapter interface for blockchain SQL services.
pub trait BlockchainSqlApi: Send + Sync + Clone + 'static {
    type ExecutionResultCursor: Send;

    /// Gets the maximum results a full query can return.
    fn max_results() -> u32;

    /// Triggers the execution of a query that maps all block ranges.
    /// Returns the Dune execution id.
    fn execute_get_block_ranges(
        &self,
        filter: &str,
        network: &str,
    ) -> impl Future<Output = SqlClientResult<String>> + Send;

    /// Triggers the execution of a query that maps all logs.
    /// Returns the Dune execution id.
    fn execute_get_logs(
        &self,
        filter: &str,
    ) -> impl Future<Output = SqlClientResult<String>> + Send;

    fn get_execution_status(
        &self,
        execution_id: &String,
    ) -> impl Future<Output = SqlClientResult<QueryExecutionStatus>> + Send;

    fn get_execution_results<T: for<'a> Deserialize<'a>>(
        &self,
        execution_id: &String,
        cursor: Option<Self::ExecutionResultCursor>,
    ) -> impl Future<Output = SqlClientResult<(Vec<T>, Option<Self::ExecutionResultCursor>)>> + Send;

    /// Polls a query result until it's completed or propagate an error if the query fails.
    fn await_for_completed(
        &self,
        logger: &Logger,
        execution_id: &String,
        polling_interval: u64,
    ) -> impl Future<Output = SqlClientResult<i64>> + Send {
        async move {
            let mut last_status: Option<QueryExecutionStatus> = None;
            let mut poll_count: u32 = 0u32;
            let start_time: Instant = Instant::now();

            loop {
                let status: QueryExecutionStatus = self.get_execution_status(&execution_id).await?;

                if Some(status) != last_status || poll_count % 30 == 0 {
                    let time: u64 = start_time.elapsed().as_secs();
                    info!(
                        logger,
                        "({}s) execution_status on execution {execution_id}: {status:?}", time
                    );
                    last_status = Some(status);
                }
                poll_count += 1;

                match status {
                    QueryExecutionStatus::Pending | QueryExecutionStatus::Executing => {}
                    QueryExecutionStatus::Completed { size } => {
                        return Ok(size);
                    }
                    QueryExecutionStatus::Failed => {
                        return Err(SqlClientError::ExecutionStatusError("Failed".into()))
                    }
                    QueryExecutionStatus::Cancelled => {
                        return Err(SqlClientError::ExecutionStatusError("Cancelled".into()))
                    }
                    QueryExecutionStatus::Expired => {
                        return Err(SqlClientError::ExecutionStatusError("Expired".into()))
                    }
                }

                time::sleep(Duration::from_secs(polling_interval)).await;
            }
        }
    }

    /// Interacts with the api to get all results from a specific query filter.
    fn execute_query_and_get_results(
        &self,
        logger: &Logger,
        filter: &Box<dyn SqlFilterWithCursor>,
    ) -> impl Future<Output = SqlClientResult<Vec<LogData>>> + Send
    where
        Self: Sync,
    {
        async move {
            let logger = logger.new(o!("component" => "BlockchainSqlApi"));

            // Counts how many logs are there and decide whether to use Dune.

            let network = filter.network();

            let size = {
                let execution_id = self
                    .execute_get_block_ranges(&filter.to_sql(), &network)
                    .await?;
                let _size = self.await_for_completed(&logger, &execution_id, 1).await?;
                let (block_ranges, _) = self
                    .get_execution_results::<BlockRange>(&execution_id, None)
                    .await?;

                block_ranges
                    .into_iter()
                    .fold(0u64, |sum, current| sum + current.logs_count)
            };

            // Aborts if the query is over the limit.
            if size >= 10_000_000 {
                return Err(SqlClientError::TooManyLogsError(size));
            }

            // Requests the query to run with the specified
            let execution_id = self.execute_get_logs(&filter.to_sql()).await?;
            let size = self.await_for_completed(&logger, &execution_id, 1).await?;

            // Aggregate all results from get_execution_results in a single vector
            let mut results: Vec<LogData> = Vec::new();
            if size > 0 {
                let mut cursor = None;
                loop {
                    let (mut new_results, next_cursor) =
                        self.get_execution_results(&execution_id, cursor).await?;
                    results.append(&mut new_results);
                    if next_cursor.is_none() {
                        break;
                    }
                    cursor = next_cursor;
                }
            }

            info!(
                logger,
                "got {} results from execution id \"{execution_id}\"",
                results.len()
            );
            Ok(results)
        }
    }
}
