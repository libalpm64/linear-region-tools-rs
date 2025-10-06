use crate::{
    io_utils, Chunk, PerformanceCounters, Region, RegionError, CHUNKS_PER_REGION,
    COMPRESSION_TYPE_ZLIB, EXTERNAL_FILE_COMPRESSION_TYPE, REGION_DIMENSION, SECTOR_SIZE,
};
use anyhow::{Context, Result};
use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

/// Anvil chunk location entry (4 bytes)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
struct ChunkLocation {
    offset: [u8; 3],
    sector_count: u8,
}

impl ChunkLocation {
    const SIZE: usize = 4;

    fn new(offset: u32, sector_count: u8) -> Self {
        let offset_bytes = offset.to_be_bytes();
        Self {
            offset: [offset_bytes[1], offset_bytes[2], offset_bytes[3]],
            sector_count,
        }
    }

    fn from_bytes(data: &[u8]) -> Self {
        Self {
            offset: [data[0], data[1], data[2]],
            sector_count: data[3],
        }
    }

    fn to_bytes(&self) -> [u8; Self::SIZE] {
        [self.offset[0], self.offset[1], self.offset[2], self.sector_count]
    }

    fn get_offset(&self) -> u32 {
        u32::from_be_bytes([0, self.offset[0], self.offset[1], self.offset[2]])
    }

    fn is_empty(&self) -> bool {
        self.get_offset() == 0 && self.sector_count == 0
    }
}

/// Anvil chunk data header (5 bytes)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
struct ChunkDataHeader {
    length: u32,
    compression_type: u8,
}

impl ChunkDataHeader {
    const SIZE: usize = 5;

    fn new(length: u32, compression_type: u8) -> Self {
        Self {
            length,
            compression_type,
        }
    }

    fn from_bytes(data: &[u8]) -> Self {
        let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let compression_type = data[4];
        Self {
            length,
            compression_type,
        }
    }

    fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..4].copy_from_slice(&self.length.to_be_bytes());
        bytes[4] = self.compression_type;
        bytes
    }
}

pub fn read_anvil_region<P: AsRef<Path>>(
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

    if file_size < SECTOR_SIZE * 2 {
        return Err(RegionError::InvalidFormat.into());
    }

    // Parse chunk locations (first 4KB)
    let mut chunk_locations = Vec::with_capacity(CHUNKS_PER_REGION);
    for i in 0..CHUNKS_PER_REGION {
        let start = i * ChunkLocation::SIZE;
        let end = start + ChunkLocation::SIZE;
        let location = ChunkLocation::from_bytes(&mmap[start..end]);
        chunk_locations.push(location);
    }

    // Parse timestamps (second 4KB)
    let mut timestamps = Vec::with_capacity(CHUNKS_PER_REGION);
    for i in 0..CHUNKS_PER_REGION {
        let start = SECTOR_SIZE + i * 4;
        let _end = start + 4;
        let timestamp = u32::from_be_bytes([
            mmap[start], mmap[start + 1], mmap[start + 2], mmap[start + 3]
        ]);
        timestamps.push(timestamp);
    }

    let mut region = Region::new(region_x, region_z);
    region.mtime = std::fs::metadata(path)?.modified()?
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    region.timestamps.extend_from_slice(&timestamps);

    let source_dir = path.parent().unwrap_or_else(|| Path::new("."));

    let mut chunks_loaded = 0u64;
    for (i, location) in chunk_locations.iter().enumerate() {
        if location.is_empty() {
            continue;
        }

        let sector_offset = location.get_offset() as usize;
        let sector_count = location.sector_count as usize;
        
        if sector_offset == 0 || sector_count == 0 {
            continue;
        }

        let chunk_start = sector_offset * SECTOR_SIZE;
        let chunk_end = chunk_start + sector_count * SECTOR_SIZE;
        
        if chunk_end > file_size {
            continue;
        }

        let chunk_data = &mmap[chunk_start..chunk_end];
        
        if chunk_data.len() < ChunkDataHeader::SIZE {
            continue;
        }

        let header = ChunkDataHeader::from_bytes(&chunk_data[..ChunkDataHeader::SIZE]);
        let compressed_data = &chunk_data[ChunkDataHeader::SIZE..];
        
        let chunk_x = region_x * REGION_DIMENSION as i32 + (i % REGION_DIMENSION) as i32;
        let chunk_z = region_z * REGION_DIMENSION as i32 + (i / REGION_DIMENSION) as i32;

        let nbt_data = match header.compression_type {
            COMPRESSION_TYPE_ZLIB => {
                let data_length = std::cmp::min(header.length as usize, compressed_data.len());
                let mut decoder = ZlibDecoder::new(&compressed_data[..data_length]);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)
                    .context("Failed to decompress zlib chunk")?;
                decompressed
            }
            EXTERNAL_FILE_COMPRESSION_TYPE => {
                let external_path = source_dir.join(format!("c.{}.{}.mcc", chunk_x, chunk_z));
                let external_mmap = io_utils::mmap_file(&external_path)
                    .with_context(|| format!("Failed to read external file: {:?}", external_path))?;
                
                let mut decoder = ZlibDecoder::new(&external_mmap[..]);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)
                    .context("Failed to decompress external chunk")?;
                decompressed
            }
            _ => {
                return Err(RegionError::InvalidFormat.into());
            }
        };

        let chunk = Chunk::new(nbt_data, chunk_x, chunk_z);
        region.set_chunk(i, chunk, timestamps[i]);
        chunks_loaded += 1;
    }

    if let Some(ref counters) = counters {
        counters.add_file();
        counters.add_chunks(chunks_loaded);
    }

    Ok(region)
}

pub fn write_anvil_region<P: AsRef<Path>>(
    path: P,
    region: &Region,
    compression_level: u32,
    counters: Option<Arc<PerformanceCounters>>,
) -> Result<()> {
    let path = path.as_ref();
    let destination_dir = path.parent().unwrap_or_else(|| Path::new("."));

    let mut chunk_locations = Vec::with_capacity(CHUNKS_PER_REGION);
    let mut sector_data = Vec::new();
    let mut current_sector = 2;

    for i in 0..CHUNKS_PER_REGION {
        if let Some(chunk) = region.get_chunk(i) {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(compression_level));
            encoder.write_all(chunk.as_slice())
                .context("Failed to write chunk data to compressor")?;
            let compressed = encoder.finish()
                .context("Failed to compress chunk data")?;

            let data_size = ChunkDataHeader::SIZE + compressed.len();
            let sectors_needed = (data_size + SECTOR_SIZE - 1) / SECTOR_SIZE;

            if sectors_needed > 255 {
                let chunk_x = region.region_x * REGION_DIMENSION as i32 + (i % REGION_DIMENSION) as i32;
                let chunk_z = region.region_z * REGION_DIMENSION as i32 + (i / REGION_DIMENSION) as i32;
                let external_path = destination_dir.join(format!("c.{}.{}.mcc", chunk_x, chunk_z));
                
                io_utils::atomic_write(&external_path, &compressed)?;
                io_utils::set_mtime(&external_path, region.mtime)?;

                let header = ChunkDataHeader::new(1, EXTERNAL_FILE_COMPRESSION_TYPE);
                let mut sector_chunk = Vec::with_capacity(SECTOR_SIZE);
                sector_chunk.extend_from_slice(&header.to_bytes());
                sector_chunk.resize(SECTOR_SIZE, 0); // Pad to sector boundary

                chunk_locations.push(ChunkLocation::new(current_sector as u32, 1));
                sector_data.extend_from_slice(&sector_chunk);
                current_sector += 1;
            } else {
                let header = ChunkDataHeader::new(compressed.len() as u32 + 1, COMPRESSION_TYPE_ZLIB);
                let mut sector_chunk = Vec::with_capacity(sectors_needed * SECTOR_SIZE);
                
                sector_chunk.extend_from_slice(&header.to_bytes());
                sector_chunk.extend_from_slice(&compressed);
                
                let padding = sectors_needed * SECTOR_SIZE - sector_chunk.len();
                sector_chunk.resize(sector_chunk.len() + padding, 0);

                chunk_locations.push(ChunkLocation::new(current_sector as u32, sectors_needed as u8));
                sector_data.extend_from_slice(&sector_chunk);
                current_sector += sectors_needed;
            }
        } else {
            chunk_locations.push(ChunkLocation::new(0, 0));
        }
    }

    let mut file_data = Vec::with_capacity(SECTOR_SIZE * 2 + sector_data.len());

    for location in &chunk_locations {
        file_data.extend_from_slice(&location.to_bytes());
    }
    file_data.resize(SECTOR_SIZE, 0);

    for &timestamp in &region.timestamps {
        file_data.extend_from_slice(&timestamp.to_be_bytes());
    }
    file_data.resize(SECTOR_SIZE * 2, 0);

    file_data.extend_from_slice(&sector_data);

    io_utils::atomic_write(path, &file_data)?;
    
    io_utils::set_mtime(path, region.mtime)?;

    if let Some(ref counters) = counters {
        counters.add_file();
        counters.add_bytes_written(file_data.len() as u64);
        counters.add_chunks(region.chunk_count() as u64);
    }

    Ok(())
}

pub fn region_to_anvil_bytes(region: &Region, compression_level: u32) -> Result<Vec<u8>> {
    let mut chunk_locations = Vec::with_capacity(CHUNKS_PER_REGION);
    let mut sector_data = Vec::new();
    let mut current_sector = 2;

    for i in 0..CHUNKS_PER_REGION {
        if let Some(chunk) = region.get_chunk(i) {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(compression_level));
            encoder.write_all(chunk.as_slice())
                .context("Failed to write chunk data to compressor")?;
            let compressed = encoder.finish()
                .context("Failed to compress chunk data")?;

            let data_size = ChunkDataHeader::SIZE + compressed.len();
            let sectors_needed = (data_size + SECTOR_SIZE - 1) / SECTOR_SIZE;

            if sectors_needed > 255 {
                return Err(RegionError::InvalidFormat.into());
            }

            let header = ChunkDataHeader::new(compressed.len() as u32 + 1, COMPRESSION_TYPE_ZLIB);
            let mut sector_chunk = Vec::with_capacity(sectors_needed * SECTOR_SIZE);
            
            sector_chunk.extend_from_slice(&header.to_bytes());
            sector_chunk.extend_from_slice(&compressed);
            
            let padding = sectors_needed * SECTOR_SIZE - sector_chunk.len();
            sector_chunk.resize(sector_chunk.len() + padding, 0);

            chunk_locations.push(ChunkLocation::new(current_sector as u32, sectors_needed as u8));
            sector_data.extend_from_slice(&sector_chunk);
            current_sector += sectors_needed;
        } else {
            chunk_locations.push(ChunkLocation::new(0, 0));
        }
    }

    let mut file_data = Vec::with_capacity(SECTOR_SIZE * 2 + sector_data.len());

    for location in &chunk_locations {
        file_data.extend_from_slice(&location.to_bytes());
    }
    file_data.resize(SECTOR_SIZE, 0);

    for &timestamp in &region.timestamps {
        file_data.extend_from_slice(&timestamp.to_be_bytes());
    }
    file_data.resize(SECTOR_SIZE * 2, 0);
    file_data.extend_from_slice(&sector_data);

    Ok(file_data)
}