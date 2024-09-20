use graph::prelude::*;

use crate::sql_client::core::SqlClientError::JsonDecodeError;
use graph::prelude::reqwest::{Error as ReqwestError, StatusCode};
use serde_json::Error as SerdeJsonError;
use thiserror::Error;

pub type SqlClientResult<T> = Result<T, SqlClientError>;

#[derive(Error, Debug)]
pub enum SqlClientError {
    #[error("error from Reqwest library: {0}")]
    ReqwestError(ReqwestError),
    #[error("http request failed with {0}: {1}")]
    ResponseError(StatusCode, String),
    #[error("error decoding response into JSON: {0}.")]
    JsonDecodeError(SerdeJsonError),
    #[error("the query failed to execute: ")]
    ExecutionStatusError(String),
    #[error("other error: {0}")]
    OtherError(String),
}

impl From<ReqwestError> for SqlClientError {
    fn from(value: ReqwestError) -> Self {
        SqlClientError::ReqwestError(value)
    }
}

impl From<SerdeJsonError> for SqlClientError {
    fn from(value: SerdeJsonError) -> Self {
        JsonDecodeError(value)
    }
}
