use anyhow::{Result, anyhow};
use clap::Parser;
use rocksdb::{ColumnFamilyDescriptor, Options, TransactionDB, TransactionDBOptions};
use serde::{Deserialize, Serialize};

const BINARY_VERSION: u8 = 0x01;
const SM_META_CF: &str = "StateMachineMetadata";
const SM_META_KEY: &[u8] = b"sm_meta";

#[derive(Debug, Serialize, Deserialize)]
struct StateMachineMetadata {
    db_version: u64,
    last_change_idx: u64,
    #[serde(default)]
    last_usage_idx: u64,
    #[serde(default)]
    last_request_event_idx: u64,
}

#[derive(Parser)]
#[command(about = "Set the state store db_version so migrations re-run on next startup")]
struct Cli {
    /// Path to the RocksDB database directory
    db_path: String,

    /// Target version to set (default: 22)
    #[arg(long, default_value_t = 22)]
    version: u64,
}

fn decode(bytes: &[u8]) -> Result<StateMachineMetadata> {
    if bytes.is_empty() {
        return Err(anyhow!("empty metadata"));
    }
    if bytes[0] == BINARY_VERSION {
        postcard::from_bytes(&bytes[1..]).map_err(|e| anyhow!("postcard decode: {e}"))
    } else {
        serde_json::from_slice(bytes).map_err(|e| anyhow!("json decode: {e}"))
    }
}

fn encode(meta: &StateMachineMetadata) -> Result<Vec<u8>> {
    let payload = postcard::to_allocvec(meta).map_err(|e| anyhow!("postcard encode: {e}"))?;
    let mut buf = Vec::with_capacity(1 + payload.len());
    buf.push(BINARY_VERSION);
    buf.extend_from_slice(&payload);
    Ok(buf)
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut db_opts = Options::default();
    db_opts.create_if_missing(false);
    db_opts.create_missing_column_families(true);

    let cfs = rocksdb::DB::list_cf(&db_opts, &cli.db_path)
        .map_err(|e| anyhow!("failed to list column families: {e}"))?;

    if !cfs.iter().any(|cf| cf == SM_META_CF) {
        return Err(anyhow!(
            "column family '{SM_META_CF}' not found — is this an indexify state store?"
        ));
    }

    let cf_descriptors: Vec<_> = cfs
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
        .collect();

    let db = TransactionDB::<rocksdb::MultiThreaded>::open_cf_descriptors(
        &db_opts,
        &TransactionDBOptions::default(),
        &cli.db_path,
        cf_descriptors,
    )
    .map_err(|e| anyhow!("failed to open database: {e}"))?;

    let cf = db
        .cf_handle(SM_META_CF)
        .ok_or_else(|| anyhow!("missing cf handle for '{SM_META_CF}'"))?;

    let raw = db
        .get_cf(&cf, SM_META_KEY)
        .map_err(|e| anyhow!("failed to read sm_meta: {e}"))?
        .ok_or_else(|| anyhow!("no sm_meta key found in '{SM_META_CF}'"))?;

    let mut meta = decode(&raw)?;
    println!("current db_version: {}", meta.db_version);

    if meta.db_version == cli.version {
        println!("already at version {} — nothing to do", cli.version);
        return Ok(());
    }

    meta.db_version = cli.version;
    let encoded = encode(&meta)?;
    db.put_cf(&cf, SM_META_KEY, &encoded)
        .map_err(|e| anyhow!("failed to write sm_meta: {e}"))?;

    println!("db_version set to {}", cli.version);
    Ok(())
}
