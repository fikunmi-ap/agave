#![allow(clippy::arithmetic_side_effects)]
use {
    agave_banking_stage_ingress_types::BankingPacketBatch,
    anyhow::Result,
    assert_matches::assert_matches,
    clap::Parser,
    crossbeam_channel::{unbounded, Receiver},
    log::*,
    solana_accounts_db::accounts_db::ACCOUNTS_DB_CONFIG_FOR_BENCHMARKS,
    solana_banking_bench_historical::{
        snapshot::download::download_snapshot,
        transactions,
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_core::{
        banking_stage::{update_bank_forks_and_poh_recorder_for_new_tpu_bank, BankingStage},
        banking_trace::{BankingTracer, Channels, BANKING_TRACE_DIR_DEFAULT_BYTE_LIMIT},
        validator::{BlockProductionMethod, TransactionStructure},
    },
    solana_gossip::cluster_info::{ClusterInfo, Node},
    solana_ledger::{
        blockstore::Blockstore,
        get_tmp_ledger_path_auto_delete,
        leader_schedule_cache::LeaderScheduleCache,
    },
    solana_measure::measure::Measure,
    solana_perf::packet::{to_packet_batches, PacketBatch},
    solana_poh::poh_recorder::{create_test_recorder, PohRecorder, WorkingBankEntry},
    solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        prioritization_fee_cache::PrioritizationFeeCache,
        runtime_config::RuntimeConfig,
        snapshot_archive_info::{FullSnapshotArchiveInfo, SnapshotArchiveInfoGetter},
        snapshot_bank_utils::bank_from_snapshot_archives,
    },
    solana_sdk::{
        hash::Hash,
        signature::{Keypair, Signer},
        genesis_config::create_genesis_config,
    },
    solana_streamer::socket::SocketAddrSpace,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::sleep,
        time::{Duration, Instant},
    },
};

#[derive(Parser)]
struct CliArgs {
    /// RPC URL to connect to target cluster.
    #[arg(short, long, default_value = "https://api.mainnet-beta.solana.com")]
    url: String,

    /// Enable banking trace.
    #[arg(short, long, action = clap::ArgAction::SetTrue)]
    trace_banking: bool,

    /// Block production method.
    #[arg(
        short,
        long,
        value_parser = clap::builder::PossibleValuesParser::new(BlockProductionMethod::cli_names()),
        default_value = "central-scheduler-greedy"
    )]
    block_production_method: BlockProductionMethod,

    /// Number of execution threads.
    #[arg(short, long, default_value_t = 4)]
    num_execution_threads: u32,

    /// Transaction structure.
    #[arg(
        short,
        long,
        value_parser = clap::builder::PossibleValuesParser::new(TransactionStructure::cli_names()),
        default_value = "sdk",
    )]
    transaction_structure: TransactionStructure,

    /// Number of blocks to reexecute.
    #[arg(short, long, default_value_t = 10)]
    num_blocks: u64,

    /// Directory to unpack the snapshot to
    #[arg(short, long, default_value_os_t = default_snapshot_path())]
    working_dir: PathBuf,
}

/// Get the path where the snapshot should be saved to.
/// The snapshot will be unpacked to the same directory.
///
/// Currently saves to the PWD.
/// TODO: Implement better logic. Can use `directories` package.
fn default_snapshot_path() -> PathBuf {
    PathBuf::from(std::env::current_dir().expect("Failed to get current dir.")).join("snapshot")
}

#[tokio::main]
async fn main() -> Result<()> {
    solana_logger::setup();
    let args = CliArgs::parse();

    let block_production_method = args.block_production_method;
    let transaction_struct = args.transaction_structure;
    let num_execution_threads = args.num_execution_threads;
    let working_dir = args.working_dir;
    let rpc_url = args.url;

    // Setup
    // - Fetch snapshot metadata,
    // - Determine if it makes sense to replay,
    // - Download blocks,
    // - Create bank.

    let rpc_client = RpcClient::new(rpc_url.clone());
    let reqwest_client = reqwest::Client::new();

    let snapshot_dir = working_dir.join("snapshots");
    let downloaded_snapshot_path =
        download_snapshot(&reqwest_client, &rpc_url, &working_dir).await?;
    let accounts_dir = working_dir.join("accounts");
    let snapshot_archive_info = FullSnapshotArchiveInfo::new_from_path(downloaded_snapshot_path)?;

    let snapshot_slot = snapshot_archive_info.snapshot_archive_info().slot;

    let (genesis_config, _) = create_genesis_config(5000);

    let (replay_vote_sender, _replay_vote_receiver) = unbounded();

    let (snapshot_bank, _) = bank_from_snapshot_archives(
        &[accounts_dir],
        &snapshot_dir,
        &snapshot_archive_info,
        None,
        &genesis_config,
        &RuntimeConfig::default(),
        None,
        None,
        None,
        false,
        false,
        false,
        false,
        Some(ACCOUNTS_DB_CONFIG_FOR_BENCHMARKS),
        None,
        Arc::new(AtomicBool::new(false)),
    )?;

    let snapshot_bank_forks = BankForks::new_rw_arc(snapshot_bank);
    let mut bank = snapshot_bank_forks
        .read()
        .unwrap()
        .working_bank_with_scheduler();

    // set cost tracker limits to MAX so it will not filter out TXs
    bank.write_cost_tracker()
        .unwrap()
        .set_limits(u64::MAX, u64::MAX, u64::MAX);

    let transactions = transactions::download_decode_and_filter_blocks(
        &rpc_client,
        snapshot_slot,
        args.num_blocks,
    )
    .await?;

    let num_transactions = transactions.len();

    info!(
        "Number of Executing Threads: {}, Number of Transactions: {}",
        args.num_execution_threads, num_transactions
    );

    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore =
        Arc::new(Blockstore::open(ledger_path.path()).expect("Failed to get database ledger"));

    let bank_for_benches = Bank::new_for_benches(&genesis_config);

    let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank_for_benches));

    let (exit, poh_recorder, transaction_recorder, poh_service, signal_receiver) =
        create_test_recorder(
            bank.clone(),
            blockstore.clone(),
            None,
            Some(leader_schedule_cache),
        );
    
    let (banking_tracer, tracer_thread) = BankingTracer::new(args.trace_banking.then_some((
        &blockstore.banking_trace_path(),
        exit.clone(),
        BANKING_TRACE_DIR_DEFAULT_BYTE_LIMIT,
    )))
    .unwrap();

    let prioritization_fee_cache = Arc::new(PrioritizationFeeCache::new(0u64));

    let cluster_info = Arc::new({
        let keypair = Arc::new(Keypair::new());
        let node = Node::new_localhost_with_pubkey(&keypair.pubkey());
        ClusterInfo::new(node.info, keypair, SocketAddrSpace::Unspecified)
    });

    let Channels {
        non_vote_sender,
        non_vote_receiver,
        tpu_vote_sender,
        tpu_vote_receiver,
        gossip_vote_sender,
        gossip_vote_receiver,
    } = banking_tracer.create_channels(false);

    let banking_stage = BankingStage::new_num_threads(
        block_production_method,
        transaction_struct,
        &cluster_info,
        &poh_recorder,
        transaction_recorder,
        non_vote_receiver,
        tpu_vote_receiver,
        gossip_vote_receiver,
        num_execution_threads,
        None,
        replay_vote_sender,
        None,
        snapshot_bank_forks.clone(),
        &prioritization_fee_cache,
    );

    // This is so that the signal_receiver does not go out of scope after the closure.
    // If it is dropped before poh_service, then poh_service will error when
    // calling send() on the channel.

    let signal_receiver = Arc::new(signal_receiver);

    let collector = solana_sdk::pubkey::new_rand();

    let packet_batch = Packets::new(transactions);

    let now = Instant::now();

    non_vote_sender.send(BankingPacketBatch::new(packet_batch.packet_batch.clone()))?;

    for tx in &packet_batch.transactions {
        loop {
            if bank.get_signature_status(&tx.signatures[0]).is_some() {
                break;
            }
            if poh_recorder.read().unwrap().bank().is_none() {
                break;
            }
            sleep(Duration::from_millis(5));
        }
    }

    if check_txs(&signal_receiver, num_transactions, &poh_recorder) {
        let tx_total_us = now.elapsed().as_micros();
        eprintln!(
            "[num_transaction: {}, time_taken_in_seconds: {}]",
            num_transactions, tx_total_us,
        );

        let mut poh_time = Measure::start("poh_time");
        poh_recorder
            .write()
            .unwrap()
            .reset(bank.clone(), Some((bank.slot(), bank.slot() + 1)));
        poh_time.stop();

        let mut new_bank_time = Measure::start("new_bank");
        if let Some((result, _timings)) = bank.wait_for_completed_scheduler() {
            assert_matches!(result, Ok(_));
        }
        let new_slot = bank.slot() + 1;
        let new_bank = Bank::new_from_parent(bank.clone(), &collector, new_slot);
        new_bank_time.stop();

        let mut insert_time = Measure::start("insert_time");
        assert_matches!(poh_recorder.read().unwrap().bank(), None);
        update_bank_forks_and_poh_recorder_for_new_tpu_bank(
            &snapshot_bank_forks,
            &poh_recorder,
            new_bank,
            false,
        );

        bank = snapshot_bank_forks
            .read()
            .unwrap()
            .working_bank_with_scheduler();
        assert_matches!(poh_recorder.read().unwrap().bank(), Some(_));
        insert_time.stop();

        debug!(
            "new_bank_time: {}us insert_time: {}us poh_time: {}us",
            new_bank_time.as_us(),
            insert_time.as_us(),
            poh_time.as_us(),
        );

        bank.clear_signatures(); // Inserted to make rust analyser happy
    } else {
    }

    drop(non_vote_sender);
    drop(tpu_vote_sender);
    drop(gossip_vote_sender);
    exit.store(true, Ordering::Relaxed);
    banking_stage.join().unwrap();
    debug!("waited for banking_stage");
    poh_service.join().unwrap();
    sleep(Duration::from_secs(1));
    debug!("waited for poh_service");
    if let Some(tracer_thread) = tracer_thread {
        tracer_thread.join().unwrap().unwrap();
    }
    Ok(())
}

/// Convienience data structure representing a `Vec` of packets.
///
/// TODO: Implement as Vec<Packet> instead of PacketBatches
struct Packets {
    packet_batch: Vec<PacketBatch>,
    transactions: Vec<VersionedTransaction>,
}

impl Packets {
    fn new(transactions: Vec<VersionedTransaction>) -> Self {
        let packet_batch = to_packet_batches(&transactions, 1);
        Self {
            packet_batch,
            transactions,
        }
    }

    fn _refresh_blockhash(&mut self, new_blockhash: Hash) {
        for tx in self.transactions.iter_mut() {
            tx.message.set_recent_blockhash(new_blockhash);
        }
        self.packet_batch = to_packet_batches(&self.transactions, 1)
    }
}

// Stops after 60 s
fn check_txs(
    receiver: &Arc<Receiver<WorkingBankEntry>>,
    ref_tx_count: usize,
    poh_recorder: &Arc<RwLock<PohRecorder>>,
) -> bool {
    let mut total = 0;
    let now = Instant::now();
    let mut no_bank = false;
    loop {
        if let Ok((_bank, (entry, _tick_height))) = receiver.recv_timeout(Duration::from_millis(10))
        {
            total += entry.transactions.len();
        }
        if total >= ref_tx_count {
            break;
        }
        if now.elapsed().as_secs() > 60 {
            break;
        }
        if poh_recorder.read().unwrap().bank().is_none() {
            no_bank = true;
            break;
        }
    }
    if !no_bank {
        assert!(total >= ref_tx_count);
    }
    no_bank
}