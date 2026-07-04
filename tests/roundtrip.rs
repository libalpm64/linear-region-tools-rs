use linear_region_tools::{
    anvil::{read_anvil_region, write_anvil_region},
    linear::{read_linear_region, write_linear_region, LinearVersion},
    Chunk, Region,
};

fn fake_nbt_chunk(x: i32, z: i32) -> Chunk {
    let nbt = fastnbt::nbt!({
        "xPos": x,
        "zPos": z,
        "Entities": [],
    });
    let data = fastnbt::to_bytes(&nbt).unwrap();
    Chunk::new(data, x, z)
}

#[test]
fn mca_linear_roundtrip() {
    let dir = std::env::temp_dir().join("lrt_roundtrip_test");
    std::fs::create_dir_all(&dir).unwrap();

    let mut region = Region::new(-1, 2);
    for &(x, z) in &[(-32, 64), (-17, 70), (-1, 95)] {
        region.set_chunk_at(x, z, fake_nbt_chunk(x, z), 12345);
    }

    let mca_path = dir.join("r.-1.2.mca");
    let linear_path = dir.join("r.-1.2.linear");

    write_anvil_region(&mca_path, &region, 6, None).unwrap();
    let from_mca = read_anvil_region(&mca_path, None).unwrap();
    assert_eq!(from_mca.chunk_count(), 3);
    assert_eq!(from_mca.timestamps.len(), 1024);

    write_linear_region(&linear_path, &from_mca, 6, LinearVersion::V1, None).unwrap();
    let from_linear = read_linear_region(&linear_path, None).unwrap();
    assert_eq!(from_linear.chunk_count(), 3);

    for &(x, z) in &[(-32, 64), (-17, 70), (-1, 95)] {
        let orig = region.get_chunk_at(x, z).unwrap();
        let round = from_linear.get_chunk_at(x, z).unwrap();
        assert_eq!(orig.as_slice(), round.as_slice(), "chunk ({x},{z}) data mismatch");
        let idx = ((z & 31) as usize) * 32 + ((x & 31) as usize);
        assert_eq!(from_linear.timestamps[idx], 12345);
    }

    std::fs::remove_dir_all(&dir).unwrap();
}
