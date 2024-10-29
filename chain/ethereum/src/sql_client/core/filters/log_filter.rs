use graph::blockchain::SubgraphSqlFilterTrait;
use graph::prelude::*;

use graph::itertools::Itertools;
use graph::prelude::ethabi::ethereum_types::H256;
use web3::types::Address;

/// Represents a filter to applied in one single client query. The client usually have a limit on how many
/// logs can be returned from one query; so, many queries might be needed with different cursors.
pub struct SubgraphSqlFilter {
    /// The last synced block on the subgraph.
    pub last_synced_block: Option<i32>,

    pub data_sources: Vec<DataSourceSqlFilter>,
}

pub struct DataSourceSqlFilter {
    /// The network, analogous to the manifest.
    pub network: String,
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

impl SubgraphSqlFilterTrait for SubgraphSqlFilter {
    fn chain_id(&self) -> String {
        if self.data_sources.len() == 0 {
            String::new()
        } else {
            // Since all datasources must have the same network, it's safe to use the network of the first one.
            self.data_sources[0].network.clone()
        }
    }

    fn to_sql(&self) -> String {
        let mut result = String::new();

        // Continue from the last synced block, if any.
        if let Some(block_number) = self.last_synced_block {
            let clause = format!("(block_number > {block_number}) AND ");
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
