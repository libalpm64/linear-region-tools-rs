use crate::{
    io_utils, Chunk, PerformanceCounters, Region, RegionError, CHUNKS_PER_REGION,
    LINEAR_SIGNATURE, LINEAR_VERSION, REGION_DIMENSION,
};
use anyhow::{Context, Result};
use std::io;
use std::path::Path;
use std::sync::Arc;
use zstd;

fn decompress_with_retry(compressed_data: &[u8], header: &LinearHeader) -> Result<Vec<u8>> {
    let mut last_error = String::new();
    
    match zstd::bulk::decompress(compressed_data, 0) {
        Ok(data) => return Ok(data),
        Err(e) => last_error = format!("Standard decompression failed: {}", e),
    }

    match zstd::bulk::decompress(compressed_data, 64 * 1024 * 1024) {
        Ok(data) => return Ok(data),
        Err(e) => last_error = format!("Limited decompression failed: {}", e),
    }

    let estimated_size = (header.chunk_count as usize) * 1024 * 16;
    match zstd::bulk::decompress(compressed_data, estimated_size) {
        Ok(data) => return Ok(data),
        Err(e) => last_error = format!("Estimated size decompression failed: {}", e),
    }

    match zstd::stream::Decoder::new(compressed_data) {
        Ok(mut decoder) => {
            let mut decompressed = Vec::new();
            match std::io::copy(&mut decoder, &mut decompressed) {
                Ok(_) => return Ok(decompressed),
                Err(e) => last_error = format!("Streaming decompression failed: {}", e),
            }
        }
        Err(e) => last_error = format!("Streaming decoder creation failed: {}", e),
    }
    Err(RegionError::DecompressionFailed { reason: last_error }.into())
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
struct LinearHeader {
    signature: u64,           // 8 bytes - LINEAR_SIGNATURE
    version: u8,              // 1 byte - LINEAR_VERSION
    newest_timestamp: u64,    // 8 bytes - newest chunk timestamp
    compression_level: i8,    // 1 byte - ZSTD compression level
    chunk_count: u16,         // 2 bytes - number of chunks in region
    compressed_size: u32,     // 4 bytes - size of compressed data
}

impl LinearHeader {
    const SIZE: usize = 24;

    fn new(newest_timestamp: u64, compression_level: i8, chunk_count: u16, compressed_size: u32) -> Self {
        Self {
            signature: LINEAR_SIGNATURE,
            version: LINEAR_VERSION,
            newest_timestamp,
            compression_level,
            chunk_count,
            compressed_size,
        }
    }

    fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < Self::SIZE {
            return Err(RegionError::InvalidFormat.into());
        }

        let signature = u64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let version = data[8];
        let newest_timestamp = u64::from_be_bytes([
            data[9], data[10], data[11], data[12], data[13], data[14], data[15], data[16],
        ]);
        let compression_level = data[17] as i8;
        let chunk_count = u16::from_be_bytes([data[18], data[19]]);
        let compressed_size = u32::from_be_bytes([data[20], data[21], data[22], data[23]]);

        Ok(Self {
            signature,
            version,
            newest_timestamp,
            compression_level,
            chunk_count,
            compressed_size,
        })
    }

    fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..8].copy_from_slice(&self.signature.to_be_bytes());
        bytes[8] = self.version;
        bytes[9..17].copy_from_slice(&self.newest_timestamp.to_be_bytes());
        bytes[17] = self.compression_level as u8;
        bytes[18..20].copy_from_slice(&self.chunk_count.to_be_bytes());
        bytes[20..24].copy_from_slice(&self.compressed_size.to_be_bytes());
        bytes
    }
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
struct ChunkMeta {
    size: u32,
    timestamp: u32,
}

impl ChunkMeta {
    const SIZE: usize = 8;

    fn from_bytes(data: &[u8]) -> Self {
        let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let timestamp = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        Self { size, timestamp }
    }

    fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..4].copy_from_slice(&self.size.to_be_bytes());
        bytes[4..8].copy_from_slice(&self.timestamp.to_be_bytes());
        bytes
    }
}

pub fn read_linear_region<P: AsRef<Path>>(
    path: P,
    counters: Option<Arc<PerformanceCounters>>,
) -> Result<Region> {
    let path = path.as_ref();
    
    let filename = path.file_name()
        .and_then(|n| n.to_str())
        .context("Invalid filename")?;
    let (region_x, region_z) = Region::parse_filename(filename)?;

    let mmap = io_utils::mmap_file(path)?;
    let file_size = mmap.len();
    
    if let Some(ref counters) = counters {
        counters.add_bytes_read(file_size as u64);
    }

    if file_size < LinearHeader::SIZE + 8 {
        return Err(RegionError::InvalidFormat.into());
    }

    let header = LinearHeader::from_bytes(&mmap[..LinearHeader::SIZE])?;
    
    if header.signature != LINEAR_SIGNATURE {
        return Err(RegionError::InvalidSignature {
            expected: LINEAR_SIGNATURE,
            found: header.signature,
        }.into());
    }

    if header.version != 1 && header.version != 2 {
        return Err(RegionError::UnsupportedVersion {
            version: header.version,
        }.into());
    }

    let footer_start = file_size - 8;
    let footer_signature = u64::from_be_bytes([
        mmap[footer_start],
        mmap[footer_start + 1],
        mmap[footer_start + 2],
        mmap[footer_start + 3],
        mmap[footer_start + 4],
        mmap[footer_start + 5],
        mmap[footer_start + 6],
        mmap[footer_start + 7],
    ]);

    if footer_signature != LINEAR_SIGNATURE {
        return Err(RegionError::InvalidSignature {
            expected: LINEAR_SIGNATURE,
            found: footer_signature,
        }.into());
    }

    let compressed_start = LinearHeader::SIZE + 8;
    let compressed_end = footer_start;
    let compressed_data = &mmap[compressed_start..compressed_end];
    let decompressed = decompress_with_retry(compressed_data, &header)?;

    let expected_header_size = CHUNKS_PER_REGION * ChunkMeta::SIZE;
    if decompressed.len() < expected_header_size {
        return Err(RegionError::InvalidFormat.into());
    }

    let mut chunk_metas = Vec::with_capacity(CHUNKS_PER_REGION);
    let mut total_chunk_size = 0usize;
    let mut real_chunk_count = 0u16;

    for i in 0..CHUNKS_PER_REGION {
        let meta_start = i * ChunkMeta::SIZE;
        let meta_end = meta_start + ChunkMeta::SIZE;
        let meta = ChunkMeta::from_bytes(&decompressed[meta_start..meta_end]);
        
        if meta.size > 0 {
            real_chunk_count += 1;
            total_chunk_size += meta.size as usize;
        }
        
        chunk_metas.push(meta);
    }

    if real_chunk_count != header.chunk_count {
        return Err(RegionError::InvalidChunkCount {
            expected: header.chunk_count,
            found: real_chunk_count,
        }.into());
    }

    if expected_header_size + total_chunk_size != decompressed.len() {
        return Err(RegionError::InvalidFormat.into());
    }

    let mut region = Region::new(region_x, region_z);
    region.mtime = std::fs::metadata(path)?.modified()?
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    let mut chunk_data_offset = expected_header_size;
    
    for (i, meta) in chunk_metas.iter().enumerate() {
        region.timestamps[i] = meta.timestamp;
        
        if meta.size > 0 {
            let chunk_start = chunk_data_offset;
            let chunk_end = chunk_start + meta.size as usize;
            let chunk_data = &decompressed[chunk_start..chunk_end];
            
            let x = region_x * REGION_DIMENSION as i32 + (i % REGION_DIMENSION) as i32;
            let z = region_z * REGION_DIMENSION as i32 + (i / REGION_DIMENSION) as i32;
            
            let chunk = Chunk::from_slice(chunk_data, x, z);
            region.set_chunk(i, chunk, meta.timestamp);
            
            chunk_data_offset = chunk_end;
        }
    }

    if let Some(ref counters) = counters {
        counters.add_file();
        counters.add_chunks(real_chunk_count as u64);
    }

    Ok(region)
}

pub fn write_linear_region<P: AsRef<Path>>(
    path: P,
    region: &Region,
    compression_level: i32,
    counters: Option<Arc<PerformanceCounters>>,
) -> Result<()> {
    let path = path.as_ref();

    let mut chunk_metas = Vec::with_capacity(CHUNKS_PER_REGION);
    let mut chunk_data = Vec::new();
    let mut newest_timestamp = 0u32;
    let mut chunk_count = 0u16;

    for i in 0..CHUNKS_PER_REGION {
        if let Some(chunk) = region.get_chunk(i) {
            let size = chunk.size() as u32;
            let timestamp = region.timestamps[i];
            
            chunk_metas.push(ChunkMeta { size, timestamp });
            chunk_data.extend_from_slice(chunk.as_slice());
            
            newest_timestamp = newest_timestamp.max(timestamp);
            chunk_count += 1;
        } else {
            chunk_metas.push(ChunkMeta { size: 0, timestamp: region.timestamps[i] });
        }
    }

    let mut decompressed = Vec::with_capacity(
        CHUNKS_PER_REGION * ChunkMeta::SIZE + chunk_data.len()
    );

    for meta in &chunk_metas {
        decompressed.extend_from_slice(&meta.to_bytes());
    }

    decompressed.extend_from_slice(&chunk_data);

    let compressed = zstd::bulk::compress(&decompressed, compression_level)
        .map_err(|e| RegionError::CompressionFailed { reason: format!("ZSTD compression failed: {}", e) })?;

    let header = LinearHeader::new(
        newest_timestamp as u64,
        compression_level as i8,
        chunk_count,
        compressed.len() as u32,
    );

    let mut file_data = Vec::with_capacity(
        LinearHeader::SIZE + 8 + compressed.len() + 8
    );
    file_data.extend_from_slice(&header.to_bytes());
    
    file_data.extend_from_slice(&[0u8; 8]);
    
    file_data.extend_from_slice(&compressed);
    
    file_data.extend_from_slice(&LINEAR_SIGNATURE.to_be_bytes());

    io_utils::atomic_write(path, &file_data)?;
    
    io_utils::set_mtime(path, region.mtime)?;

    if let Some(ref counters) = counters {
        counters.add_file();
        counters.add_bytes_written(file_data.len() as u64);
        counters.add_chunks(chunk_count as u64);
    }

    Ok(())
}

pub fn verify_linear_file<P: AsRef<Path>>(path: P) -> bool {
    let Ok(mmap) = io_utils::mmap_file(path) else {
        return false;
    };

    if mmap.len() < LinearHeader::SIZE + 8 {
        return false;
    }

    let Ok(header) = LinearHeader::from_bytes(&mmap[..LinearHeader::SIZE]) else {
        return false;
    };

    if header.signature != LINEAR_SIGNATURE || (header.version != 1 && header.version != 2) {
        return false;
    }

    let footer_start = mmap.len() - 8;
    let footer_signature = u64::from_be_bytes([
        mmap[footer_start],
        mmap[footer_start + 1],
        mmap[footer_start + 2],
        mmap[footer_start + 3],
        mmap[footer_start + 4],
        mmap[footer_start + 5],
        mmap[footer_start + 6],
        mmap[footer_start + 7],
    ]);

    footer_signature == LINEAR_SIGNATURE
}