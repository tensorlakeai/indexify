use std::path::PathBuf;

use sha2::{Digest, Sha256};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
};

pub struct SnapshotReceiver {
    file_path: PathBuf,
    writer: BufWriter<File>,
    total_bytes_written: usize,
    hash: Sha256,
    pub buffer: Vec<u8>,
}

impl SnapshotReceiver {
    pub async fn new(snapshot_path: &PathBuf) -> Result<Self, anyhow::Error> {
        let nanoid = nanoid::nanoid!();
        let snapshot_file = snapshot_path.with_extension(format!("{nanoid}.stream.tmp"));
        let file = File::create(&snapshot_file).await?;
        let writer = BufWriter::new(file);
        let hash = Sha256::new();
        let buffer = Vec::new();

        Ok(Self {
            file_path: snapshot_file,
            writer,
            total_bytes_written: 0,
            hash,
            buffer,
        })
    }

    pub async fn write_chunk(&mut self, bytes: &[u8]) -> Result<(), anyhow::Error> {
        self.writer.write_all(bytes).await?;
        self.total_bytes_written += bytes.len();
        self.hash.update(bytes);
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<usize, anyhow::Error> {
        self.writer.flush().await?;
        self.writer.get_ref().sync_all().await?;
        self.writer.shutdown().await?;
        Ok(self.total_bytes_written)
    }

    pub async fn read_data(&mut self) -> Result<Vec<u8>, anyhow::Error> {
        let mut file = File::open(&self.file_path).await?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;

        let mut read_hash = Sha256::new();
        read_hash.update(&data);

        // Clone the hash object before finalizing to avoid consuming it
        let hash_clone = self.hash.clone();
        let write_hash = format!("{:x}", hash_clone.finalize());
        let read_hash = format!("{:x}", read_hash.finalize());

        if write_hash != read_hash {
            return Err(anyhow::anyhow!(
                "Data corruption detected: hashes do not match"
            ));
        } else {
            println!("The hashes match");
        }

        Ok(data)
    }
}
