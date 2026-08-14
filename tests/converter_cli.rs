use std::process::Command;

#[cfg(unix)]
use linear_region_tools::{Region, anvil::write_anvil_region};

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

#[cfg(unix)]
#[test]
fn converter_follows_region_file_symlinks() {
    use std::os::unix::fs::symlink;

    let root = std::env::temp_dir().join(format!("lrt_cli_symlink_{}", std::process::id()));
    let source = root.join("source");
    let backing = root.join("backing");
    let destination = root.join("destination");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&source).unwrap();
    std::fs::create_dir_all(&backing).unwrap();

    let backing_region = backing.join("r.0.0.mca");
    write_anvil_region(&backing_region, &Region::new(0, 0), 6, None).unwrap();
    symlink(&backing_region, source.join("r.0.0.mca")).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_convert_region_files"))
        .arg("mca2linear")
        .arg(&source)
        .arg(&destination)
        .arg("--verify")
        .arg("--threads")
        .arg("1")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "symlinked region conversion failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(destination.join("r.0.0.linear").is_file());

    std::fs::remove_dir_all(&root).unwrap();
}
