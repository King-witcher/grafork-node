use std::time::Instant;

use graph::{log, prelude::*, tokio_stream::StreamExt};

use graph_chain_ethereum::sql_client::{
    filters::{DataSourceSqlFilter, EventHandlerSqlFilter, SubgraphSqlFilter},
    apis::dune::DuneApi,    
    BlockchainSqlApi, QueryStream,
};
use web3::types::H256;

#[tokio::main]
async fn main() {
    let logger = log::logger(true);
    let api = DuneApi::new(&logger);

    stream_and_consume(&logger, api).await;
}

trait FromSignature {
    fn from_sig(signature: &str) -> Self;
}

impl FromSignature for H256 {
    fn from_sig(signature: &str) -> Self {
        let hash = web3::signing::keccak256(signature.as_bytes());
        H256::from_slice(&hash)
    }
}

impl FromSignature for EventHandlerSqlFilter {
    fn from_sig(signature: &str) -> Self {
        EventHandlerSqlFilter {
            topic0: H256::from_sig(signature),
            topic1: vec![],
            topic2: vec![],
            topic3: vec![],
        }
    }
}

async fn stream_and_consume(logger: &Logger, api: impl BlockchainSqlApi) {
    let mut stream = QueryStream::from_sql_api(logger, cheap_filter(), api);
    let mut count = 0u64;
    // consume the whole stream
    let time = Instant::now();
    while let Some(_data) = stream.next().await {
        count += 1;
    }
    info!(
        logger,
        "stream consumed {} events in {}m",
        count,
        time.elapsed().as_secs() / 60
    );
}

fn cheap_filter() -> SubgraphSqlFilter {
    SubgraphSqlFilter {
        cursor: None,
        data_sources: vec![DataSourceSqlFilter {
            contract_address: None,
            start_block: Some(10_000_000),
            end_block: Some(10_000_040),
            event_handlers: vec![EventHandlerSqlFilter {
                topic0: H256::from_sig("Transfer(address,address,uint256)"),
                topic1: vec![],
                topic2: vec![],
                topic3: vec![],
            }],
        }],
    }
}

fn _orium_filter() -> SubgraphSqlFilter {
    SubgraphSqlFilter {
        cursor: None,
        data_sources: vec![
            DataSourceSqlFilter {
                start_block: Some(52697079),
                end_block: None,
                contract_address: Some(String::from("0xb1d47b09aa6d81d7b00c3a37705a6a157b83c49f").parse().unwrap()),
                event_handlers: vec![
                    EventHandlerSqlFilter::from_sig(
                        "RentalOfferCreated(uint256,address,uint256,uint256,uint256,address,address,address,uint256,uint256,bytes32[],bytes[])"
                    ),
                    EventHandlerSqlFilter::from_sig(
                        "RentalOfferCreated(uint256,address,uint256,uint256,uint256,address,address,address,uint256,uint256,uint64,bytes32[],bytes[])"
                    ),
                    EventHandlerSqlFilter::from_sig("RentalOfferCancelled(address,uint256)"),
                    EventHandlerSqlFilter::from_sig("RentalStarted(address,uint256,address,uint64)"),
                    EventHandlerSqlFilter::from_sig("RentalEnded(address,uint256)"),
                ],
            },
            DataSourceSqlFilter {
                start_block: Some(55374011),
                end_block: None,
                contract_address: Some(String::from("0x1fbaf746747addd76b70c76cd1069ffcab1b7be4").parse().unwrap()),
                event_handlers: vec![
                    EventHandlerSqlFilter::from_sig("CreatorRoyaltySet(address,address,uint256,address)"),
                    EventHandlerSqlFilter::from_sig("MarketplaceFeeSet(address,uint256,bool)"),
                    EventHandlerSqlFilter::from_sig("RolesRegistrySet(address,address)"),
                ],
            },
            DataSourceSqlFilter {
                start_block: Some(52696971),
                end_block: Some(55374011),
                contract_address: Some(String::from("0x060ec866243596851938c8cb42d023d9f7017862").parse().unwrap()),
                event_handlers: vec![
                    EventHandlerSqlFilter::from_sig("CreatorRoyaltySet(address,address,uint256,address)"),
                    EventHandlerSqlFilter::from_sig("MarketplaceFeeSet(address,uint256,bool)"),
                    EventHandlerSqlFilter::from_sig("RolesRegistrySet(address,address)"),
                ],
            },
            DataSourceSqlFilter {
                contract_address: Some(String::from("0x86935f11c86623dec8a25696e1c19a8659cbf95d").parse().unwrap()),
                start_block: Some(53748199),
                end_block: None,
                event_handlers: vec![
                    EventHandlerSqlFilter::from_sig("RoleGranted(uint256,bytes32,address,uint64,bool,bytes)"),
                    EventHandlerSqlFilter::from_sig("RoleRevoked(uint256,bytes32,address)"),
                    EventHandlerSqlFilter::from_sig("RoleApprovalForAll(address,address,bool)"),
                    EventHandlerSqlFilter::from_sig("TokensCommitted(address,uint256,address,uint256,uint256)"),
                    EventHandlerSqlFilter::from_sig("TokensReleased(uint256)"),
                ],
            },
            DataSourceSqlFilter {
                contract_address: Some(String::from("0x287b0e4ef86ffa5818f5f7e3d950679f43ef0890").parse().unwrap()),
                start_block: Some(58992454),
                end_block: None,
                event_handlers: vec![
                    EventHandlerSqlFilter::from_sig("RoleGranted(uint256,bytes32,address,uint64,bool,bytes)"),
                    EventHandlerSqlFilter::from_sig("RoleRevoked(uint256,bytes32,address)"),
                    EventHandlerSqlFilter::from_sig("RoleApprovalForAll(address,address,bool)"),
                    EventHandlerSqlFilter::from_sig("TokensLocked(address,uint256,address,uint256,uint256)"),
                    EventHandlerSqlFilter::from_sig("TokensUnlocked(uint256)"),
                ],
            },
            DataSourceSqlFilter {
                contract_address: Some(String::from("0x86935f11c86623dec8a25696e1c19a8659cbf95d").parse().unwrap()),
                start_block: Some(11516320),
                end_block: None,
                event_handlers: vec![
                    EventHandlerSqlFilter::from_sig("TransferSingle(address,address,address,uint256,uint256)"),
                    EventHandlerSqlFilter::from_sig("TransferBatch(address,address,address,uint256[],uint256[])"),
                    EventHandlerSqlFilter::from_sig("ApprovalForAll(address,address,bool)"),
                ],
            },
            DataSourceSqlFilter {
                contract_address: Some(String::from("0x58de9aabcaeec0f69883c94318810ad79cc6a44f").parse().unwrap()),
                start_block: Some(35999793),
                end_block: None,
                event_handlers: vec![
                    EventHandlerSqlFilter::from_sig("TransferSingle(address,address,address,uint256,uint256)"),
                    EventHandlerSqlFilter::from_sig("TransferBatch(address,address,address,uint256[],uint256[])"),
                    EventHandlerSqlFilter::from_sig("ApprovalForAll(address,address,bool)"),
                ],
            },
            DataSourceSqlFilter {
                contract_address: Some(String::from("0x9c09596d3d3691ea971f0b40b8cad44186868267").parse().unwrap()),
                start_block: Some(23867898),
                end_block: None,
                event_handlers: vec![
                    EventHandlerSqlFilter::from_sig("TransferSingle(address,address,address,uint256,uint256)"),
                    EventHandlerSqlFilter::from_sig("TransferBatch(address,address,address,uint256[],uint256[])"),
                    EventHandlerSqlFilter::from_sig("ApprovalForAll(address,address,bool)"),
                ],
            },
            DataSourceSqlFilter {
                contract_address: Some(String::from("0x906f413f2198d9343454b26d8a02fba86105e89c").parse().unwrap()),
                start_block: Some(26076762),
                end_block: None,
                event_handlers: vec![
                    EventHandlerSqlFilter::from_sig("TransferSingle(address,address,address,uint256,uint256)"),
                    EventHandlerSqlFilter::from_sig("TransferBatch(address,address,address,uint256[],uint256[])"),
                    EventHandlerSqlFilter::from_sig("ApprovalForAll(address,address,bool)"),
                ],
            },
        ],
    }
}
