use graph::prelude::*;
use reqwest::{Client, RequestBuilder};

use crate::sql_client::core::{BlockchainSqlApi, QueryExecutionStatus, SqlClientError};
use crate::sql_client::{BlockRange, SqlClientResult};

use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

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
pub struct ExecutionResult<T> {
    pub rows: Vec<T>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetExecutionResultResponse<T> {
    query_id: i64,
    next_offset: Option<i64>,
    execution_id: String,
    result: ExecutionResult<T>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ErrorResponse {
    error: String,
}

/// Extension trait that allows to authenticate a Reqwest RequestBuilder with dune credentials
trait ReqwestExt {
    fn authenticate(self) -> Self;
}

impl ReqwestExt for RequestBuilder {
    fn authenticate(self) -> Self {
        self.header("x-dune-api-key", &ENV_VARS.dune_api_key)
    }
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

    async fn execute_query(
        &self,
        query_id: &str,
        parameters: serde_json::Value,
    ) -> SqlClientResult<String> {
        info!(
            self.logger,
            "running query {} with parameters {}", query_id, parameters
        );

        let url = format!("{}/query/{}/execute", BASE_URL, query_id);

        let json_body = json!({
            "query_parameters": parameters,
            "performance": DUNE_ENGINE_TYPE,
        });

        let req = self.reqwest.post(url).json(&json_body).authenticate();
        let res = req.send().await?;

        let status = res.status();
        let mut object = res.json::<HashMap<String, String>>().await?;
        match status.is_success() {
            true => {
                let execution_id = object.remove("execution_id").unwrap();
                info!(self.logger, "got execution id {execution_id} for query {query_id} with parameters {parameters}");
                Ok(execution_id)
            }
            false => {
                let message = object.remove("error").unwrap_or(String::from("unknown"));
                error!(self.logger, "failed to execute query on Dune: {message}");
                Err(SqlClientError::ResponseError(status, message))
            }
        }
    }

    fn get_dune_blockchain_id(network: &str) -> SqlClientResult<String> {
        match network {
            "mainnet" => Ok(String::from("ethereum")),
            "matic" => Ok(String::from("polygon")),
            "arbitrum-one" => Ok(String::from("arbitrum")),
            "optimism" => Ok(String::from("optimism")),
            _ => Err(SqlClientError::NotSupportedNetwork(String::from(network))),
        }
    }
}

impl BlockchainSqlApi for DuneApi {
    type ExecutionResultCursor = i64;

    fn max_results() -> u32 {
        ENV_VARS.dune_query_limit
    }

    async fn execute_get_block_ranges(
        &self,
        filter: &str,
        network: &str,
    ) -> SqlClientResult<String> {
        info!(self.logger, "querying block ranges on Dune");

        let blockchain_id = DuneApi::get_dune_blockchain_id(network)?;
        let params = json!({
            "blockchain_id": blockchain_id,
            "chunk_size": &ENV_VARS.dune_query_limit,
            "filter": filter,
        });

        self.execute_query(&ENV_VARS.dune_block_ranges_query, params)
            .await
    }

    async fn execute_get_logs(
        &self,
        filter: &str,
        chain_id: &str,
        block_range: &BlockRange,
    ) -> SqlClientResult<String> {
        if let Some(execution_id) = &ENV_VARS.dune_force_execution_id {
            warn!(
                self.logger,
                "skipping get_logs because execution_id is forced to be {execution_id}"
            );
            return Ok(String::from(execution_id));
        }

        info!(self.logger, "querying logs on Dune with filter {filter}");
        let blockchain_id = DuneApi::get_dune_blockchain_id(chain_id)?;

        let params = json!({
            "filter": filter,
            "blockchain_id": blockchain_id,
            "start_log_index": block_range.start_log_index.parse::<i64>().unwrap(),
            "end_log_index": block_range.end_log_index.parse::<i64>().unwrap(),
            "start_block_number": block_range.start_block_number.parse::<i64>().unwrap(),
            "end_block_number": block_range.end_block_number.parse::<i64>().unwrap(),
            "limit": ENV_VARS.dune_query_limit.to_string(),
        });

        self.execute_query(&ENV_VARS.dune_logs_query, params).await
    }

    async fn get_execution_status(
        &self,
        execution_id: &String,
    ) -> SqlClientResult<QueryExecutionStatus> {
        let execution_id = ENV_VARS
            .dune_force_execution_id
            .as_ref()
            .unwrap_or(execution_id);

        let url = format!("{}/execution/{execution_id}/status", BASE_URL);
        let req = self.reqwest.get(url).authenticate();

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

    async fn get_execution_results<T: for<'a> Deserialize<'a>>(
        &self,
        execution_id: &String,
        cursor: Option<Self::ExecutionResultCursor>,
    ) -> SqlClientResult<(Vec<T>, Option<i64>)> {
        const ATTEMPT_DELAYS: [u64; 6] = [0, 1, 3, 5, 7, 11];

        let execution_id = ENV_VARS
            .dune_force_execution_id
            .as_ref()
            .unwrap_or(execution_id);
        let cursor = cursor.unwrap_or(0);

        info!(self.logger, "requesting {PAGE_SIZE} results from execution id \"{execution_id}\" (offset = {cursor})");

        let url = format!(
            "{}/execution/{}/results/?offset={}&limit={PAGE_SIZE}",
            BASE_URL, execution_id, cursor
        );

        let mut retries = 0;

        let res = loop {
            sleep(Duration::from_secs(ATTEMPT_DELAYS[retries])).await;

            let req = self.reqwest.get(&url).authenticate();

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
                let object: GetExecutionResultResponse<T> = serde_json::from_str(&text)?;

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
