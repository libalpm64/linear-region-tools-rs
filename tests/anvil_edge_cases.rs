use linear_region_tools::{
    Chunk, Region,
    anvil::{read_anvil_region, write_anvil_region},
};
use std::io::Write;

fn test_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("lrt_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn pseudo_random_data(size: usize, mut state: u32) -> Vec<u8> {
    let mut data = vec![0u8; size];
    for byte in &mut data {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        *byte = state as u8;
    }
    data
}

#[test]
fn large_external_anvil_chunk_roundtrips_without_loss() {
    let dir = test_dir("external_chunk");
    let path = dir.join("r.0.0.mca");
    let mut region = Region::new(0, 0);
    let data = pseudo_random_data(1_100_000, 0x1234_5678);
    region.set_chunk_at(0, 0, Chunk::new(data.clone(), 0, 0), 99);

    write_anvil_region(&path, &region, 6, None).unwrap();
    assert!(dir.join("c.0.0.mcc").exists());
    let roundtrip = read_anvil_region(&path, None).unwrap();
    assert_eq!(roundtrip.get_chunk_at(0, 0).unwrap().as_slice(), data);

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn forge_style_255_sector_sentinel_reads_the_full_local_chunk() {
    let dir = test_dir("forge_oversized_local");
    let path = dir.join("r.0.0.mca");
    let data = pseudo_random_data(1_100_000, 0x9abc_def0);

    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::new(1));
    encoder.write_all(&data).unwrap();
    let compressed = encoder.finish().unwrap();
    let stored_size = 5 + compressed.len();
    let sectors = stored_size.div_ceil(linear_region_tools::SECTOR_SIZE);
    assert!(sectors > 255);

    let mut raw = vec![0u8; (2 + sectors) * linear_region_tools::SECTOR_SIZE];
    raw[0..4].copy_from_slice(&[0, 0, 2, 255]);
    raw[4096..4100].copy_from_slice(&77u32.to_be_bytes());
    let chunk_start = 2 * linear_region_tools::SECTOR_SIZE;
    raw[chunk_start..chunk_start + 4].copy_from_slice(&(compressed.len() as u32 + 1).to_be_bytes());
    raw[chunk_start + 4] = linear_region_tools::COMPRESSION_TYPE_ZLIB;
    raw[chunk_start + 5..chunk_start + 5 + compressed.len()].copy_from_slice(&compressed);
    std::fs::write(&path, raw).unwrap();

    let region = read_anvil_region(&path, None).unwrap();
    assert_eq!(region.get_chunk_at(0, 0).unwrap().as_slice(), data);
    assert_eq!(region.timestamps[0], 77);

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn truncated_anvil_location_is_an_error_instead_of_an_empty_chunk() {
    let dir = test_dir("truncated_anvil");
    let path = dir.join("r.0.0.mca");
    let mut raw = vec![0u8; 8192];
    raw[0..4].copy_from_slice(&[0, 0, 2, 1]);
    std::fs::write(&path, raw).unwrap();

    let error = match read_anvil_region(&path, None) {
        Ok(_) => panic!("truncated chunk was silently accepted"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("beyond file size"),
        "unexpected error: {error}"
    );

    std::fs::remove_dir_all(&dir).unwrap();
}
