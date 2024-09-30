use graph::prelude::*;

use crate::sql_client::core::data::LogData;
use graph::blockchain::ToSqlFilter;
use std;
use std::cmp::PartialEq;
use std::future::Future;
use std::time::{Duration, Instant};
use tokio;
use tokio::time;
use web3::futures::Stream;

use crate::sql_client::core::SqlClientError;

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

/// Represents an adapter interface for blockchain SQL services.
pub trait BlockchainSqlApi: Send + Sync + Clone + 'static {
    type ExecutionResultCursor: Send;

    /// Gets the maximum results a full query can return.
    fn max_results() -> u32;

    /// Requests a query to be executed and returns an execution id that can be used to poll its
    /// execution state and get the result.
    fn execute_query(
        &self,
        filter: &str,
    ) -> impl Future<Output = Result<String, SqlClientError>> + Send;

    fn get_execution_status(
        &self,
        execution_id: &str,
    ) -> impl Future<Output = Result<QueryExecutionStatus, SqlClientError>> + Send;

    fn get_execution_result(
        &self,
        execution_id: &str,
        cursor: Option<Self::ExecutionResultCursor>,
    ) -> impl Future<
        Output = Result<(Vec<LogData>, Option<Self::ExecutionResultCursor>), SqlClientError>,
    > + Send;

    /// Interacts with the api to get all results from a specific query filter.
    fn execute_query_and_get_results(
        &self,
        logger: &Logger,
        filter: &Box<dyn ToSqlFilter>,
        polling_interval: u64,
    ) -> impl Future<Output = Result<Vec<LogData>, SqlClientError>> + Send
    where
        Self: Sync,
    {
        async move {
            let logger = logger.new(o!("component" => "BlockchainSqlApi"));

            // Requests the query to run with the specified
            let execution_id = self.execute_query(&filter.to_sql()).await?;

            // Polls the get_execution_status endpoint until it's ready
            let mut last_status: Option<QueryExecutionStatus> = None;
            let mut poll_count = 0u32;
            let start_time = Instant::now();
            let _size = loop {
                let status = self.get_execution_status(&execution_id).await?;

                if Some(status) != last_status || poll_count % 30 == 0 {
                    let time = start_time.elapsed().as_secs();
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
                        break Ok(size);
                    }
                    QueryExecutionStatus::Failed => {
                        break Err(SqlClientError::ExecutionStatusError("Failed".into()))
                    }
                    QueryExecutionStatus::Cancelled => {
                        break Err(SqlClientError::ExecutionStatusError("Cancelled".into()))
                    }
                    QueryExecutionStatus::Expired => {
                        break Err(SqlClientError::ExecutionStatusError("Expired".into()))
                    }
                }

                time::sleep(Duration::from_secs(polling_interval)).await;
            }?;

            let mut results: Vec<LogData> = Vec::new();
            let mut cursor = None;

            loop {
                let (mut new_results, next_cursor) =
                    self.get_execution_result(&execution_id, cursor).await?;
                results.append(&mut new_results);
                if next_cursor.is_none() {
                    break;
                }
                cursor = next_cursor;
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
