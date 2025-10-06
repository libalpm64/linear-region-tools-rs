use anyhow::{Context, Result};
use memmap2::Mmap;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

pub mod anvil;
pub mod linear;
pub mod nbt;

pub const REGION_DIMENSION: usize = 32;
pub const CHUNKS_PER_REGION: usize = REGION_DIMENSION * REGION_DIMENSION;
pub const SECTOR_SIZE: usize = 4096;
pub const LINEAR_SIGNATURE: u64 = 0xc3ff13183cca9d9a;
pub const LINEAR_VERSION: u8 = 1;
pub const COMPRESSION_TYPE_ZLIB: u8 = 2;
pub const EXTERNAL_FILE_COMPRESSION_TYPE: u8 = 128 + 2;
type ChunkData = SmallVec<[u8; 8192]>;

#[derive(Error, Debug)]
pub enum RegionError {
    #[error("Invalid signature: expected {expected:#x}, found {found:#x}")]
    InvalidSignature { expected: u64, found: u64 },
    
    #[error("Unsupported version: {version}")]
    UnsupportedVersion { version: u8 },
    
    #[error("Invalid chunk count: expected {expected}, found {found}")]
    InvalidChunkCount { expected: u16, found: u16 },
    
    #[error("Decompression failed: {reason}")]
    DecompressionFailed { reason: String },
    
    #[error("Compression failed: {reason}")]
    CompressionFailed { reason: String },
    
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Invalid file format")]
    InvalidFormat,
}

#[derive(Clone)]
pub struct Chunk {
    pub data: ChunkData,
    pub x: i32,
    pub z: i32,
}

impl Chunk {
    #[inline]
    pub fn new(data: Vec<u8>, x: i32, z: i32) -> Self {
        Self {
            data: SmallVec::from_vec(data),
            x,
            z,
        }
    }

    #[inline]
    pub fn from_slice(data: &[u8], x: i32, z: i32) -> Self {
        Self {
            data: SmallVec::from_slice(data),
            x,
            z,
        }
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn parse_nbt(&self) -> Result<fastnbt::Value> {
        fastnbt::from_bytes(&self.data).context("Failed to parse NBT data")
    }

    pub fn from_nbt(nbt: &fastnbt::Value, x: i32, z: i32) -> Result<Self> {
        let data = fastnbt::to_bytes(nbt).context("Failed to serialize NBT data")?;
        Ok(Self::new(data, x, z))
    }
}

pub struct Region {
    pub chunks: HashMap<usize, Chunk, ahash::RandomState>,
    pub region_x: i32,
    pub region_z: i32,
    pub mtime: u64,
    pub timestamps: SmallVec<[u32; CHUNKS_PER_REGION]>,
}

impl Region {
    pub fn new(region_x: i32, region_z: i32) -> Self {
        Self {
            chunks: HashMap::with_hasher(ahash::RandomState::new()),
            region_x,
            region_z,
            mtime: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            timestamps: SmallVec::from_elem(0, CHUNKS_PER_REGION),
        }
    }

    #[inline]
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    #[inline]
    pub fn get_chunk(&self, index: usize) -> Option<&Chunk> {
        self.chunks.get(&index)
    }

    #[inline]
    pub fn set_chunk(&mut self, index: usize, chunk: Chunk, timestamp: u32) {
        self.timestamps[index] = timestamp;
        self.chunks.insert(index, chunk);
    }

    #[inline]
    pub fn remove_chunk(&mut self, index: usize) {
        self.chunks.remove(&index);
        self.timestamps[index] = 0;
    }

    #[inline]
    pub fn get_chunk_at(&self, x: i32, z: i32) -> Option<&Chunk> {
        let local_x = x & 31;
        let local_z = z & 31;
        let index = (local_z as usize) * REGION_DIMENSION + (local_x as usize);
        self.get_chunk(index)
    }

    #[inline]
    pub fn set_chunk_at(&mut self, x: i32, z: i32, chunk: Chunk, timestamp: u32) {
        let local_x = x & 31;
        let local_z = z & 31;
        let index = (local_z as usize) * REGION_DIMENSION + (local_x as usize);
        self.set_chunk(index, chunk, timestamp);
    }

    pub fn parse_filename(filename: &str) -> Result<(i32, i32)> {
        let parts: Vec<&str> = filename.split('.').collect();
        if parts.len() < 3 {
            return Err(RegionError::InvalidFormat.into());
        }
        
        let region_x = parts[1].parse::<i32>()
            .context("Invalid region X coordinate")?;
        let region_z = parts[2].parse::<i32>()
            .context("Invalid region Z coordinate")?;
        
        Ok((region_x, region_z))
    }
}

/// Performance counters for monitoring
pub struct PerformanceCounters {
    pub files_processed: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
    pub chunks_processed: AtomicU64,
}

impl PerformanceCounters {
    pub fn new() -> Self {
        Self {
            files_processed: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            chunks_processed: AtomicU64::new(0),
        }
    }

    pub fn add_file(&self) {
        self.files_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_bytes_read(&self, bytes: u64) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn add_bytes_written(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn add_chunks(&self, chunks: u64) {
        self.chunks_processed.fetch_add(chunks, Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> PerformanceStats {
        PerformanceStats {
            files_processed: self.files_processed.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            chunks_processed: self.chunks_processed.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceStats {
    pub files_processed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub chunks_processed: u64,
}

impl Default for PerformanceCounters {
    fn default() -> Self {
        Self::new()
    }
}

pub mod io_utils {
    use super::*;

    pub fn mmap_file<P: AsRef<Path>>(path: P) -> Result<Mmap> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(mmap)
    }

    pub fn atomic_write<P: AsRef<Path>>(path: P, data: &[u8]) -> Result<()> {
        let path = path.as_ref();
        let temp_path = path.with_extension("tmp");
        
        {
            let mut file = BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&temp_path)?
            );
            file.write_all(data)?;
            file.flush()?;
            file.into_inner()?.sync_all()?;
        }
        
        std::fs::rename(temp_path, path)?;
        Ok(())
    }

    pub fn set_mtime<P: AsRef<Path>>(path: P, mtime: u64) -> Result<()> {
        let file_time = filetime::FileTime::from_unix_time(mtime as i64, 0);
        filetime::set_file_mtime(path, file_time)?;
        Ok(())
    }
}