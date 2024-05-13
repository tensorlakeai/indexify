use std::path::PathBuf;

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
};

pub struct SnapshotReceiver {
    file_path: PathBuf,
    writer: BufWriter<File>,
    total_bytes_written: usize,
}

impl SnapshotReceiver {
    pub async fn new(snapshot_path: &PathBuf) -> Result<Self, anyhow::Error> {
        let nanoid = nanoid::nanoid!();
        let snapshot_file = snapshot_path.with_extension(format!("{nanoid}.stream.tmp"));
        let file = File::create(&snapshot_file).await?;
        let writer = BufWriter::new(file);

        Ok(Self {
            file_path: snapshot_file,
            writer,
            total_bytes_written: 0,
        })
    }

    pub async fn write_chunk(&mut self, bytes: &[u8]) -> Result<(), anyhow::Error> {
        self.writer.write_all(bytes).await?;
        self.total_bytes_written += bytes.len();
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
        Ok(data)
    }
}
