use std::process::Command;

#[test]
fn converter_returns_failure_when_any_region_is_invalid() {
    let root = std::env::temp_dir().join(format!("lrt_cli_failure_{}", std::process::id()));
    let source = root.join("source");
    let destination = root.join("destination");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&source).unwrap();

    let mut truncated = vec![0u8; 8192];
    truncated[0..4].copy_from_slice(&[0, 0, 2, 1]);
    std::fs::write(source.join("r.0.0.mca"), truncated).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_convert_region_files"))
        .arg("mca2linear")
        .arg(&source)
        .arg(&destination)
        .arg("--verify")
        .arg("--threads")
        .arg("1")
        .output()
        .unwrap();

    assert!(!output.status.success(), "invalid input returned success");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("conversion failed for 1 region file"),
        "unexpected stderr: {stderr}"
    );

    std::fs::remove_dir_all(&root).unwrap();
}
