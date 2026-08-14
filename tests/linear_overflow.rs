use linear_region_tools::linear::read_linear_region;

fn test_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("lrt_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn signed_chunk_length_overflow_is_rejected_without_allocating_it() {
    let dir = test_dir("linear_chunk_overflow");
    let path = dir.join("r.0.0.linear");

    let mut payload = vec![0u8; 1024 * 8];
    payload[0..4].copy_from_slice(&0x8000_0000u32.to_be_bytes());
    let compressed = zstd::bulk::compress(&payload, 1).unwrap();

    let mut file = Vec::new();
    file.extend_from_slice(&linear_region_tools::LINEAR_SIGNATURE.to_be_bytes());
    file.push(linear_region_tools::LINEAR_VERSION);
    file.extend_from_slice(&0u64.to_be_bytes());
    file.push(1);
    file.extend_from_slice(&1u16.to_be_bytes());
    file.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
    file.extend_from_slice(&0u64.to_be_bytes());
    file.extend_from_slice(&compressed);
    file.extend_from_slice(&linear_region_tools::LINEAR_SIGNATURE.to_be_bytes());
    std::fs::write(&path, file).unwrap();

    let error = match read_linear_region(&path, None) {
        Ok(_) => panic!("signed-overflow chunk length was accepted"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("2147483648") && error.contains("2147483647"),
        "unexpected error: {error}"
    );

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn signed_region_length_overflow_is_rejected_before_slicing() {
    let dir = test_dir("linear_region_overflow");
    let path = dir.join("r.0.0.linear");

    let mut file = Vec::new();
    file.extend_from_slice(&linear_region_tools::LINEAR_SIGNATURE.to_be_bytes());
    file.push(linear_region_tools::LINEAR_VERSION);
    file.extend_from_slice(&0u64.to_be_bytes());
    file.push(1);
    file.extend_from_slice(&0u16.to_be_bytes());
    file.extend_from_slice(&0x8000_0000u32.to_be_bytes());
    file.extend_from_slice(&0u64.to_be_bytes());
    file.extend_from_slice(&linear_region_tools::LINEAR_SIGNATURE.to_be_bytes());
    std::fs::write(&path, file).unwrap();

    let error = match read_linear_region(&path, None) {
        Ok(_) => panic!("signed-overflow region length was accepted"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("2147483648") && error.contains("2147483647"),
        "unexpected error: {error}"
    );

    std::fs::remove_dir_all(&dir).unwrap();
}
