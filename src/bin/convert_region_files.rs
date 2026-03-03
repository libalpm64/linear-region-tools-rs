use anyhow::Result;
use clap::{Parser, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use linear_region_tools::{
    anvil::{read_anvil_region, write_anvil_region},
    linear::{read_linear_region, write_linear_region, LinearVersion},
};
use rayon::prelude::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, ValueEnum)]
enum ConversionMode {
    Mca2linearv1,
    Linearv12mca,
    Mca2linearv2,
    Linearv2mca,
}

#[derive(Parser)]
#[command(
    name = "convert_region_files",
    about = "Region file format converter.",
    long_about = "Convert region files between Anvil (.mca) and Linear (.linear) formats"
)]
struct Args {
    conversion_mode: ConversionMode,
    source_dir: PathBuf,
    destination_dir: PathBuf,
    #[arg(short, long, default_value_t = num_cpus::get())]
    threads: usize,
    #[arg(short, long, default_value_t = 6)]
    compression_level: i32,
    #[arg(short, long)]
    log: bool,
    #[arg(long)]
    skip_existing: bool,
    #[arg(long)]
    verify: bool,
}

struct ConversionStats {
    converted: AtomicU64,
    errors: AtomicU64,
}

impl ConversionStats {
    fn new() -> Self {
        Self {
            converted: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    fn add_converted(&self, count: u64) {
        self.converted.fetch_add(count, Ordering::Relaxed);
    }

    fn add_errors(&self, count: u64) {
        self.errors.fetch_add(count, Ordering::Relaxed);
    }
}

fn get_output_filename(mode: &ConversionMode, source_filename: &str) -> String {
    match mode {
        ConversionMode::Mca2linearv1 => source_filename.replace(".mca", ".linear"),
        ConversionMode::Linearv12mca => source_filename.replace(".linear", ".mca"),
        ConversionMode::Mca2linearv2 => source_filename.replace(".mca", ".linear"),
        ConversionMode::Linearv2mca => source_filename.replace(".linear", ".mca"),
    }
}

fn is_valid_source_file(mode: &ConversionMode, filename: &str) -> bool {
    match mode {
        ConversionMode::Mca2linearv1 | ConversionMode::Mca2linearv2 => filename.ends_with(".mca"),
        ConversionMode::Linearv12mca | ConversionMode::Linearv2mca => filename.ends_with(".linear"),
    }
}

fn get_linear_version(mode: &ConversionMode) -> LinearVersion {
    match mode {
        ConversionMode::Mca2linearv1 | ConversionMode::Linearv12mca => LinearVersion::V1,
        ConversionMode::Mca2linearv2 | ConversionMode::Linearv2mca => LinearVersion::V2,
    }
}

fn convert_file(
    mode: &ConversionMode,
    source_path: &Path,
    dest_path: &Path,
    compression_level: i32,
    skip_existing: bool,
    verify: bool,
) -> Result<()> {
    if skip_existing && dest_path.exists() {
        return Ok(());
    }

    let linear_version = get_linear_version(mode);

    match mode {
        ConversionMode::Mca2linearv1 | ConversionMode::Mca2linearv2 => {
            let region = read_anvil_region(source_path, None)?;
            if verify {
                // Verify chunks can be read
                for i in 0..1024 {
                    let _ = region.get_chunk(i);
                }
            }
            write_linear_region(dest_path, &region, compression_level, linear_version, None)?;
        }
        ConversionMode::Linearv12mca | ConversionMode::Linearv2mca => {
            let region = read_linear_region(source_path, None)?;
            if verify {
                for i in 0..1024 {
                    let _ = region.get_chunk(i);
                }
            }
            write_anvil_region(dest_path, &region, compression_level as u32, None)?;
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    if !args.source_dir.exists() {
        eprintln!(
            "Source directory does not exist: {}",
            args.source_dir.display()
        );
        std::process::exit(1);
    }

    fs::create_dir_all(&args.destination_dir)?;

    let source_files: Vec<_> = fs::read_dir(&args.source_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let filename = e.file_name().to_string_lossy().to_string();
            is_valid_source_file(&args.conversion_mode, &filename)
        })
        .collect();

    if source_files.is_empty() {
        eprintln!("No source files found");
        std::process::exit(1);
    }

    let stats = Arc::new(ConversionStats::new());
    let start = Instant::now();
    let progress = ProgressBar::new(source_files.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:.cyan}] {pos}/{len} ({percent}%) {msg}")?
            .progress_chars("#>-"),
    );

    source_files.par_iter().for_each(|source_entry| {
        let source_filename = source_entry.file_name().to_string_lossy().to_string();
        let dest_filename = get_output_filename(&args.conversion_mode, &source_filename);
        let source_path = source_entry.path();
        let dest_path = args.destination_dir.join(&dest_filename);

        progress.inc(1);

        match convert_file(
            &args.conversion_mode,
            &source_path,
            &dest_path,
            args.compression_level,
            args.skip_existing,
            args.verify,
        ) {
            Ok(_) => stats.add_converted(1),
            Err(e) => {
                if args.log {
                    eprintln!("Error converting {}: {}", source_filename, e);
                }
                stats.add_errors(1);
            }
        }
    });

    progress.finish();

    let duration = start.elapsed();
    let converted = stats.converted.load(Ordering::Relaxed);
    let errors = stats.errors.load(Ordering::Relaxed);

    println!();
    println!("Conversion Summary:");
    println!("Files converted: {}", converted);
    println!("Errors: {}", errors);
    println!("Total time: {:?}", duration);

    if duration.as_secs() > 0 {
        println!(
            "Average speed: {:.1} files/sec",
            converted as f64 / duration.as_secs_f64()
        );
    }

    Ok(())
}
