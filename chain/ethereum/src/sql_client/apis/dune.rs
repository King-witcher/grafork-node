mod dune_result_stream;

use graph::prelude::*;
use reqwest::Client;

use crate::sql_client::core::data::LogData;
use crate::sql_client::core::{BlockchainSqlApi, QueryExecutionStatus, SqlClientError};
pub use dune_result_stream::*;

use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

const LOG_QUERY_ID: &str = "4041328";
const BASE_URL: &str = "https://api.dune.com/api/v1";
const PAGE_SIZE: u16 = 10_000;
const DUNE_ENGINE_TYPE: &str = "medium";

#[derive(Debug, Clone)]
pub struct DuneApi {
    logger: Logger,
    reqwest: Client,
}

#[derive(Serialize, Deserialize, Debug)]
struct ResultMetadata {
    total_row_count: i64,
    datapoint_count: i64,
    row_count: i64,
    execution_time_millis: i64,
}

#[derive(Serialize, Deserialize, Debug)]
struct GetExecutionStatusResponse {
    result_metadata: Option<ResultMetadata>,
    state: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ExecutionResult {
    pub rows: Vec<LogData>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetExecutionResultResponse {
    query_id: i64,
    next_offset: Option<i64>,
    execution_id: String,
    result: ExecutionResult,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ErrorResponse {
    error: String,
}

impl DuneApi {
    pub fn new(logger: &Logger) -> Self {
        let client = Client::new();
        let logger = logger.new(o!("component" => "DuneApi"));

        Self {
            logger,
            reqwest: client,
        }
    }
}

impl BlockchainSqlApi for DuneApi {
    type ExecutionResultCursor = i64;

    fn max_results() -> u32 {
        ENV_VARS.dune_query_limit
    }

    async fn execute_query(&self, filter: &str) -> Result<String, SqlClientError> {
        info!(
            self.logger,
            "executing query {LOG_QUERY_ID} with filter {filter}"
        );

        let json_body = json!({
            "query_parameters": {
                "filter": format!("{}", filter)
            },
            "performance": DUNE_ENGINE_TYPE,
        });

        let req = self
            .reqwest
            .post(format!("{}/query/{}/execute", BASE_URL, LOG_QUERY_ID))
            .json(&json_body)
            .header("X-Dune-API-Key", &ENV_VARS.dune_api_key);

        let res = req.send().await?;
        let status = res.status();
        let mut object = res.json::<HashMap<String, String>>().await?;

        match status.is_success() {
            true => {
                let execution_id = object.remove("execution_id").unwrap();
                info!(self.logger, "got execution id: {execution_id}");
                Ok(execution_id)
            }
            false => {
                let message = object.remove("error").unwrap_or(String::from("unknown"));
                error!(self.logger, "error on execute query: {message}");
                Err(SqlClientError::ResponseError(status, message))
            }
        }
    }

    async fn get_execution_status(
        &self,
        execution_id: &str,
    ) -> Result<QueryExecutionStatus, SqlClientError> {
        let url = format!("{}/execution/{execution_id}/status", BASE_URL);
        let req = self
            .reqwest
            .get(url)
            .header("X-Dune-API-Key", &ENV_VARS.dune_api_key);

        let res = req.send().await?;
        let status = res.status();
        let text = res.text().await?;

        match status.is_success() {
            true => {
                let object: GetExecutionStatusResponse = serde_json::from_str(&text)?;
                let state = match object.state.as_str() {
                    "QUERY_STATE_PENDING" => QueryExecutionStatus::Pending,
                    "QUERY_STATE_EXECUTING" => QueryExecutionStatus::Executing,
                    "QUERY_STATE_FAILED" => QueryExecutionStatus::Failed,
                    "QUERY_STATE_COMPLETED" => {
                        let result_metadata = object
                            .result_metadata
                            .expect("missing expected 'result_metadata' field on COMPLETED state");
                        let time = result_metadata.execution_time_millis / 1000;
                        info!(self.logger, "execution id {execution_id} is ready in {}s and has returned {} events", time, result_metadata.total_row_count);
                        QueryExecutionStatus::Completed {
                            size: result_metadata.total_row_count,
                        }
                    }
                    "QUERY_STATE_CANCELLED" => QueryExecutionStatus::Cancelled,
                    "QUERY_STATE_EXPIRED" => QueryExecutionStatus::Expired,
                    "QUERY_STATE_COMPLETED_PARTIAL" => QueryExecutionStatus::Failed,

                    _ => QueryExecutionStatus::Failed,
                };
                Ok(state)
            }
            false => {
                let response_body = text.clone();
                let object: ErrorResponse = serde_json::from_str(&text)?;
                error!(
                    self.logger,
                    "error polling execution status: {}. response body: {}",
                    object.error,
                    response_body
                );
                Err(SqlClientError::ResponseError(status, object.error))
            }
        }
    }

    async fn get_execution_result(
        &self,
        execution_id: &str,
        cursor: Option<Self::ExecutionResultCursor>,
    ) -> Result<(Vec<LogData>, Option<i64>), SqlClientError> {
        const ATTEMPT_DELAYS: [u64; 6] = [0, 1, 3, 5, 7, 11];
        let cursor = cursor.unwrap_or(0);
        info!(self.logger, "requesting {PAGE_SIZE} results from execution id \"{execution_id}\" (offset = {cursor})");

        let url = format!(
            "{}/execution/{}/results/?offset={}&limit={PAGE_SIZE}",
            BASE_URL, execution_id, cursor
        );

        let mut retries = 0;

        let res = loop {
            sleep(Duration::from_secs(ATTEMPT_DELAYS[retries])).await;

            let req = self
                .reqwest
                .get(&url)
                .header("x-dune-api-key", &ENV_VARS.dune_api_key);

            let res = req.send().await?;
            let status = res.status();

            // If got status code 500 and there are still retry times left, then retry
            if status.is_server_error() && retries < ATTEMPT_DELAYS.len() - 1 {
                retries += 1;
                error!(
                    self.logger,
                    "received status code 500 from server. Retrying in {}s",
                    ATTEMPT_DELAYS[retries]
                );
                continue;
            }

            break res;
        };

        let status = res.status();
        let text = res.text().await?;

        match status.is_success() {
            true => {
                let object: GetExecutionResultResponse = serde_json::from_str(&text)?;

                Ok((object.result.rows, object.next_offset))
            }
            false => {
                let response: ErrorResponse = serde_json::from_str(&text)?;
                error!(
                    self.logger,
                    "failed to fetch execution results from dune due to {}", response.error
                );
                Err(SqlClientError::ResponseError(status, response.error))
            }
        }
    }
}
