use graph::prelude::*;

use graph::blockchain::ToSqlFilter;
use graph::itertools::Itertools;
use graph::prelude::ethabi::ethereum_types::H256;
use web3::types::Address;

/// Represents a filter to applied in one single client query. The client usually have a limit on how many
/// queries can be returned from one query; so, many queries might be needed with different cursors.subg
pub struct SubgraphSqlFilter {
    /// The event after which the query will start (block_number, log_index) from which the current query will be start.
    /// This is useful for paginated queries, when the api can't return all values in one query.
    pub cursor: Option<(i32, i32)>,

    pub data_sources: Vec<DataSourceSqlFilter>,
}

pub struct DataSourceSqlFilter {
    pub contract_address: Option<Address>,
    pub start_block: Option<i32>,
    pub end_block: Option<i32>,
    pub event_handlers: Vec<EventHandlerSqlFilter>,
}

pub struct EventHandlerSqlFilter {
    pub topic0: H256,
    pub topic1: Vec<H256>,
    pub topic2: Vec<H256>,
    pub topic3: Vec<H256>,
}

impl ToSqlFilter for SubgraphSqlFilter {
    fn to_sql(&self) -> String {
        let mut result = String::new();

        // Subgraph cursor
        if let Some((block_number, log_index)) = self.cursor {
            let clause = format!("((block_number = {block_number} AND index > {log_index}) OR block_number > {block_number}) AND ");
            result += &clause;
        }

        // All datasources
        {
            let datasources_clause = self
                .data_sources
                .iter()
                .map(DataSourceSqlFilter::to_sql)
                .collect_vec()
                .join(" OR ");

            result += &format!("({datasources_clause})")
        }

        result
    }
}

impl DataSourceSqlFilter {
    fn to_sql(&self) -> String {
        let mut clauses = Vec::<String>::new();

        // Contract Address
        if let Some(contract) = self.contract_address {
            let clause = format!("contract_address = {contract:#?}");
            clauses.push(clause);
        }

        // Start block
        if let Some(start_block) = self.start_block {
            let clause = format!("block_number >= {start_block}");
            clauses.push(clause);
        }

        // End block (exclusive)
        if let Some(end_block) = self.end_block {
            let clause = format!("block_number < {end_block}");
            clauses.push(clause);
        }

        // Event handlers
        {
            let handlers_clause = self
                .event_handlers
                .iter()
                .map(EventHandlerSqlFilter::to_sql)
                .collect_vec()
                .join(" OR ");

            clauses.push(format!("({})", handlers_clause));
        }

        format!("({})", clauses.join(" AND "))
    }
}

impl EventHandlerSqlFilter {
    fn to_sql(&self) -> String {
        let mut result = String::from("(");

        // Signature (Topic 0)
        {
            let clause = format!("topic0 = {:#?}", self.topic0);
            result += &clause;
        }

        // Topic 1
        if self.topic1.len() > 0 {
            let clause = Self::get_topic_clause(&self.topic1, "topic1");
            result += &format!(" AND {}", clause);
        }

        // Topic 2
        if self.topic2.len() > 0 {
            let clause = Self::get_topic_clause(&self.topic2, "topic2");
            result += &format!(" AND {}", clause);
        }

        // Topic 3
        if self.topic3.len() > 0 {
            let clause = Self::get_topic_clause(&self.topic3, "topic3");
            result += &format!(" AND {}", clause);
        }

        result + ")"
    }

    fn get_topic_clause(topic_vector: &Vec<H256>, topic_name: &str) -> String {
        let inner = topic_vector
            .iter()
            .map(|topic| format!("{topic_name} = {topic:#?}"))
            .collect_vec()
            .join(" OR ");

        format!("({inner})")
    }
}
