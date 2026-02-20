use crate::subgraph::inputs::IndexingInputs;
use anyhow::bail;
use graph::blockchain::block_stream::{BlockStream, BufferedBlockStream};
use graph::blockchain::{Blockchain, SubgraphSqlFilterTrait};
use graph::prelude::{CheapClone, Error, SubgraphInstanceMetrics};
use std::sync::Arc;

pub async fn new_block_stream<C: Blockchain>(
    inputs: &IndexingInputs<C>,
    filter: &C::TriggerFilter,
    metrics: &SubgraphInstanceMetrics,
) -> Result<Box<dyn BlockStream<C>>, Error> {
    let is_firehose = inputs.chain.chain_client().is_firehose();

    match inputs
        .chain
        .new_block_stream(
            inputs.deployment.clone(),
            inputs.store.cheap_clone(),
            inputs.start_blocks.clone(),
            Arc::new(filter.clone()),
            inputs.unified_api_version.clone(),
        )
        .await
    {
        Ok(block_stream) => Ok(BufferedBlockStream::spawn_from_stream(
            block_stream.buffer_size_hint(),
            block_stream,
        )),
        Err(e) => {
            if is_firehose {
                metrics.firehose_connection_errors.inc();
            }
            bail!(e);
        }
    }
}

pub async fn new_sql_stream<C: Blockchain>(
    inputs: &IndexingInputs<C>,
    filter: Box<dyn SubgraphSqlFilterTrait>,
    _metrics: &SubgraphInstanceMetrics,
) -> Result<Box<dyn BlockStream<C>>, Error> {
    let sql_block_stream = inputs.chain.new_sql_block_stream(filter).await?;
    Ok(BufferedBlockStream::spawn_from_stream(
        sql_block_stream.buffer_size_hint(),
        sql_block_stream,
    ))
}
