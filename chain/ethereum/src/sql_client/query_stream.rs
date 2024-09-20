use graph::prelude::*;

use crate::sql_client::core::data::LogData;
use crate::sql_client::core::filters::SubgraphSqlFilter;
use graph::futures03::{FutureExt, Stream};
use graph::petgraph::matrix_graph::Zero;
use std::collections::VecDeque;
use std::future::Future;
use std::task::{Context, Poll};
use tokio::sync::RwLock;

use crate::sql_client::core::{BlockchainSqlApi, SqlClientError};

/// A stream that interacts with a BlockchainSqlApi and converts many large queries into one single
/// stream of results.
///
/// This is necessary because many Blockchain SQL APIs have a maximum number of results that can be
/// fetched in one single query.
///
/// This stream is a state machine that queries results on demand as they are being consumed and
/// yields its results as they are being queried.
pub struct QueryStream<B: BlockchainSqlApi + Send + Sync + Clone + 'static> {
    state: StreamState,
    ctx: StreamContext<B>,
}

impl<SqlApi: BlockchainSqlApi + Send + Sync + Clone + 'static> QueryStream<SqlApi> {
    pub fn from_sql_api(logger: &Logger, filter: SubgraphSqlFilter, sql_api: SqlApi) -> Self {
        Self {
            state: StreamState::BeginReconciliation(None),
            ctx: StreamContext::<SqlApi> {
                client: Arc::new(sql_api),
                page: 1,
                logger: logger.new(o!("component" => "QueryStream")),
                filter: Arc::new(RwLock::new(filter)),
            },
        }
    }
}

enum StreamState {
    /// Either the stream has just began or the last chunk fetched from
    /// BlockchainSqlApi::execute_query_and_get_results was exhausted and the stream will make a
    /// request to fetch the next results.
    ///
    /// Next state: Reconciliation
    BeginReconciliation(Option<(i32, i32)>),

    /// The reconciliation request was made and the stream is pending.
    ///
    /// Next states: YieldingResults | Exhausted | Failed
    Reconciling(Pin<Box<dyn Future<Output = Result<Vec<LogData>, SqlClientError>> + Send>>),

    /// The reconciliation has returned a list of results that are being served by the stream.
    ///
    /// Next states: BeginReconciliation
    YieldingResults {
        results: VecDeque<LogData>,
        next_cursor: Option<(i32, i32)>,
    },

    /// The stream was exhausted and there are no more results to read.
    Exhausted,

    /// The stream failed to fetch results.
    Failed,
}

/// Stores necessary data for the stream to run that should be sent to another thread that asks for
/// results.
#[derive(Clone)]
struct StreamContext<B: BlockchainSqlApi + Send + Sync + Clone + 'static> {
    client: Arc<B>,
    logger: Logger,
    page: u64,
    filter: Arc<RwLock<SubgraphSqlFilter>>,
}

impl<B> StreamContext<B>
where
    B: BlockchainSqlApi + Send + Sync + Clone + 'static,
{
    /// Get as many results as it can from the client and returns latest block_index and event_index that was fetched.
    async fn get_results(
        &self,
        start_after: Option<(i32, i32)>,
    ) -> Result<Vec<LogData>, SqlClientError> {
        // Update filter cursor

        // Set the offset of the current query
        let filter = self.filter.clone();
        {
            let mut lock = filter.write().await;
            lock.cursor = start_after;
        }

        let filter_read = filter.read().await;

        let results = self
            .client
            .execute_query_and_get_results(&self.logger, &*filter_read, 1)
            .await?;

        Ok(results)
    }
}

impl<B> Stream for QueryStream<B>
where
    B: BlockchainSqlApi + Send + Sync + Clone + 'static,
{
    type Item = LogData;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamState::BeginReconciliation(cursor) => {
                    let cursor = cursor.clone();
                    let ctx = self.ctx.clone();
                    let max = <B as BlockchainSqlApi>::max_results();
                    info!(
                        ctx.logger,
                        "querying for {} more results with cursor {cursor:?} (page {})",
                        max,
                        self.ctx.page
                    );
                    let fut = async move { ctx.get_results(cursor).await };
                    self.state = StreamState::Reconciling(fut.boxed());
                }

                StreamState::Reconciling(future) => {
                    match future.poll_unpin(cx) {
                        Poll::Ready(Ok(results)) => {
                            // If no results were found in the last reconciliation, the full query is exhausted.
                            if results.len().is_zero() {
                                self.state = StreamState::Exhausted
                            } else {
                                // Otherwise, serve the results and save the next cursor.
                                self.ctx.page += 1;
                                let max = <B as BlockchainSqlApi>::max_results() as usize;
                                let next_cursor = if results.len() == max {
                                    // Uses the last entry as the next cursor.
                                    let last_result = &results[results.len() - 1];
                                    Some((last_result.block_number, last_result.log_index))
                                } else {
                                    // However, ff the current page of results is less than API's max results,
                                    // set next_cursor to None as the stream of results has ended.
                                    None
                                };

                                self.state = StreamState::YieldingResults {
                                    results: results.into(),
                                    next_cursor,
                                }
                            }
                        }
                        Poll::Ready(Err(error)) => {
                            error!(
                                self.ctx.logger,
                                "failed to reconcile results due to {error:?}"
                            );
                            self.state = StreamState::Failed;
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }

                StreamState::YieldingResults {
                    results,
                    next_cursor,
                } => match results.pop_front() {
                    None => {
                        if next_cursor.is_some() {
                            self.state = StreamState::BeginReconciliation(next_cursor.clone());
                        } else {
                            self.state = StreamState::Exhausted;
                        }
                        info!(
                            self.ctx.logger,
                            "results from reconciliation were exhausted"
                        );
                    }
                    Some(event_data) => return Poll::Ready(Some(event_data)),
                },
                StreamState::Exhausted => {
                    info!(self.ctx.logger, "QueryStream has been exhausted");
                    return Poll::Ready(None);
                }
                StreamState::Failed => {
                    // info!(self.ctx.logger, "Orium Stream failed due to {}", reason);
                    return Poll::Ready(None);
                }
            }
        }
    }
}
