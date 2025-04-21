//! Houses the function for downloading and decoding blocks.

use {
    anyhow::{anyhow, Result},
    bs58,
    futures::{stream, StreamExt},
    solana_client::{
        client_error::ClientError, nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig,
    },
    solana_hash::Hash,
    solana_message::{
        legacy::Message,
        v0::{self, MessageAddressTableLookup},
        VersionedMessage,
    },
    solana_program::instruction::CompiledInstruction,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_transaction::versioned::VersionedTransaction,
    solana_transaction_status_client_types::{EncodedTransaction, UiConfirmedBlock, UiMessage},
    std::str::FromStr,
};

/// Vote Program ID.
const VOTE_PROGRAM_ID: Pubkey = solana_vote_program::id();

/// Number of blocks to be downloaded concurrently.
const NUM_CONCURRENT_BLOCKS: usize = 10;

/// Downloads and decodes `num_blocks` blocks then filters
/// out vote transactions.
/// 
/// Returns a single `Vec` of `VersionedTransaction`.
pub async fn download_decode_and_filter_blocks(
    rpc_client: &RpcClient,
    slot: u64,
    num_blocks: u64,
) -> Result<Vec<VersionedTransaction>> {
    let encoded_blocks = download_blocks(rpc_client, slot, num_blocks).await?;
    let transactions = encoded_blocks
        .iter()
        .map(decode_block)
        .collect::<Result<Vec<Vec<_>>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<VersionedTransaction>>();

    Ok(filter_votes(transactions))
}

/// Downloads the first n blocks from slot `slot` concurrently.
///
/// Handles the bug that comes with versioned transactions being
/// used on mainnet.
async fn download_blocks(
    rpc_client: &RpcClient,
    slot: u64,
    num_blocks: u64,
) -> Result<Vec<UiConfirmedBlock>, ClientError> {

    stream::iter(slot..(slot + num_blocks))
        .map(|slot| async move { download_block(rpc_client, slot).await })
        .buffer_unordered(NUM_CONCURRENT_BLOCKS)
        .collect::<Vec<Result<_, _>>>()
        .await
        .into_iter()
        .collect()
}

/// Decodes a block of transactions, i.e., it transforms a
/// `UiConfirmedBlock` to a `Vec<VersionedTransaction>`.
/// 
/// Can successfully decode Mainnetbeta transactions by calling
/// `decode` implemented below.
fn decode_block(encoded_block: &UiConfirmedBlock) -> Result<Vec<VersionedTransaction>> {
    if let Some(encoded_transactions) = &encoded_block.transactions {
        encoded_transactions
            .iter()
            .map(|e_tx| decode(&e_tx.transaction))
            .collect()
    } else {
        Ok(Vec::new())
    }
}

/// Removes voting transactions from a `Vec` of `VersionedTransactions`
/// and returns a `Vec` of non-voting transactions.
///
/// Can be used even on transactions with lookups by taking advantage
/// of the constraint that program addresses must be statically defined.
fn filter_votes(transactions: Vec<VersionedTransaction>) -> Vec<VersionedTransaction> {
    transactions
        .into_iter()
        .filter(|tx| !tx.message.static_account_keys().contains(&VOTE_PROGRAM_ID))
        .collect()
}

/// Downloads the block at height `slot`.
/// 
/// Can be used reliably on mainnet because it supports
/// versioned transactions.
async fn download_block(
    rpc_client: &RpcClient,
    slot: u64,
) -> Result<UiConfirmedBlock, ClientError> {
    rpc_client
        .get_block_with_config(
            slot,
            RpcBlockConfig {
                max_supported_transaction_version: Some(0),
                ..RpcBlockConfig::default()
            },
        )
        .await
}

/// Decodes an`EncodedTransaction` fetched from Solana mainnet-beta.
///
/// For some unknown reason, at the current time, calling
/// `EncodedTransaction::decode()` on an `EncodedTransaction` from
/// mainnet-beta returns `None`. Hence the need for this function.
///
/// It extends the subset of decodable transactions by supporting the
/// `Raw(UiRawMessage),` variant of `EncodedTransaction::Json(UiTransaction)`
/// by calling the existing `EncodedTransaction::decode()` on types
/// that it will successfully decode.
fn decode(transaction: &EncodedTransaction) -> Result<VersionedTransaction> {
    let ui_transaction = match transaction {
        EncodedTransaction::Json(ui_tx) => ui_tx,
        EncodedTransaction::Accounts(_) => {
            return Err(anyhow!(
                "Decoding accounts encoded transactions is unsupported"
            ))
        }
        EncodedTransaction::Binary(..) | EncodedTransaction::LegacyBinary(_) => {
            return transaction
                .decode()
                .ok_or_else(|| anyhow!("Failed to decode suupported tx variant"))
        }
    };

    let raw_ui_message = match &ui_transaction.message {
        UiMessage::Raw(msg) => msg,
        UiMessage::Parsed(_) => return Err(anyhow!("Decoding parsed UI messages is unsupported")),
    };

    let account_keys = raw_ui_message
        .account_keys
        .iter()
        .map(|keys| Pubkey::from_str(&keys))
        .collect::<Result<Vec<_>, _>>()?;

    let recent_blockhash = Hash::from_str(&raw_ui_message.recent_blockhash)?;

    let instructions = raw_ui_message
        .instructions
        .iter()
        .map(|tx| {
            let decoded_data = bs58::decode(tx.data.clone())
                .into_vec()
                .map_err(|e| anyhow::Error::from(e))?;

            Ok(CompiledInstruction {
                accounts: tx.accounts.clone(),
                program_id_index: tx.program_id_index,
                data: decoded_data,
            })
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    let address_table_lookups = raw_ui_message
        .address_table_lookups
        .as_ref()
        .map(|lookups| {
            lookups
                .iter()
                .map(|lu| {
                    Ok(MessageAddressTableLookup {
                        account_key: Pubkey::from_str(&lu.account_key)
                            .map_err(|e| anyhow::Error::from(e))?,
                        writable_indexes: lu.writable_indexes.clone(),
                        readonly_indexes: lu.readonly_indexes.clone(),
                    })
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()
        });

    let message = match address_table_lookups {
        Some(atlus) => VersionedMessage::V0(v0::Message {
            header: raw_ui_message.header,
            account_keys,
            recent_blockhash,
            instructions,
            address_table_lookups: atlus?,
        }),
        None => VersionedMessage::Legacy(Message {
            header: raw_ui_message.header,
            account_keys,
            recent_blockhash,
            instructions,
        }),
    };

    let signatures = ui_transaction
        .signatures
        .iter()
        .map(|sig| Signature::from_str(&sig))
        .collect::<Result<Vec<Signature>, _>>()?;

    Ok(VersionedTransaction {
        message,
        signatures,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[ignore = "Downloads a file and calls unrelated functions."]
    #[tokio::test]
    async fn test_decode() {
        let rpc_client = RpcClient::new("https://api.mainnet-beta.solana.com".to_string());

        let recently_confirmed_block = rpc_client.get_block_height().await.unwrap();

        let encoded_block = download_block(&rpc_client, recently_confirmed_block)
            .await
            .unwrap();

        let decoded_transactions: Vec<_> = encoded_block
            .transactions
            .expect("Recently confirmed block does not contain transactions")
            .iter()
            .map(|e_tx| decode(&e_tx.transaction))
            .collect();

        decoded_transactions
            .iter()
            .map(|tx| assert!(tx.as_ref().unwrap().sanitize().is_ok()))
            .collect()
    }

    #[tokio::test]
    async fn test_filter_votes() {
        let transactions = vec![VersionedTransaction {
            message: VersionedMessage::V0(v0::Message {
                account_keys: vec![VOTE_PROGRAM_ID],
                ..v0::Message::default()
            }),
            ..VersionedTransaction::default()
        }];

        let filtered_tx = filter_votes(transactions);
        assert!(filtered_tx.is_empty())
    }
}
