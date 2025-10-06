use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use linear_region_tools::{
    anvil::{read_anvil_region, write_anvil_region},
    linear::{read_linear_region, write_linear_region},
    PerformanceCounters,
};
use rayon::prelude::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, ValueEnum)]
enum ConversionMode {
    Mca2linear,
    Linear2mca,
}

#[derive(Parser)]
#[command(
    name = "convert_region_files",
    about = "Ultra-fast region file format converter",
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
    skipped: AtomicU64,
    errors: AtomicU64,
    total_input_bytes: AtomicU64,
    total_output_bytes: AtomicU64,
}

impl ConversionStats {
    fn new() -> Self {
        Self {
            converted: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            total_input_bytes: AtomicU64::new(0),
            total_output_bytes: AtomicU64::new(0),
        }
    }

    fn add_converted(&self, input_size: u64, output_size: u64) {
        self.converted.fetch_add(1, Ordering::Relaxed);
        self.total_input_bytes.fetch_add(input_size, Ordering::Relaxed);
        self.total_output_bytes.fetch_add(output_size, Ordering::Relaxed);
    }

    fn add_skipped(&self) {
        self.skipped.fetch_add(1, Ordering::Relaxed);
    }

    fn add_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn get_compression_ratio(&self) -> f64 {
        let input = self.total_input_bytes.load(Ordering::Relaxed) as f64;
        let output = self.total_output_bytes.load(Ordering::Relaxed) as f64;
        if input > 0.0 {
            (output / input) * 100.0
        } else {
            0.0
        }
    }
}

fn should_convert_file(source_path: &Path, dest_path: &Path, skip_existing: bool) -> Result<bool> {
    if !skip_existing {
        return Ok(true);
    }

    let dest_metadata = match fs::metadata(dest_path) {
        Ok(metadata) => metadata,
        Err(_) => return Ok(true),
    };

    let source_metadata = fs::metadata(source_path)?;
    
    let source_mtime = source_metadata.modified()?;
    let dest_mtime = dest_metadata.modified()?;
    
    Ok(source_mtime > dest_mtime)
}

fn convert_single_file(
    source_path: PathBuf,
    dest_dir: PathBuf,
    mode: ConversionMode,
    compression_level: i32,
    skip_existing: bool,
    verify: bool,
    stats: Arc<ConversionStats>,
    counters: Arc<PerformanceCounters>,
    log_mode: bool,
) -> Result<()> {
    let source_filename = source_path.file_name()
        .and_then(|n| n.to_str())
        .context("Invalid source filename")?;

    let dest_filename = match mode {
        ConversionMode::Mca2linear => {
            source_filename.replace(".mca", ".linear")
        }
        ConversionMode::Linear2mca => {
            source_filename.replace(".linear", ".mca")
        }
    };

    let dest_path = dest_dir.join(&dest_filename);

    if !should_convert_file(&source_path, &dest_path, skip_existing)? {
        stats.add_skipped();
        return Ok(());
    }

    let source_size = fs::metadata(&source_path)?.len();
    if source_size == 0 {
        stats.add_skipped();
        return Ok(());
    }

    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let start_time = Instant::now();

    let result: Result<()> = match mode {
        ConversionMode::Mca2linear => {
            let region = read_anvil_region(&source_path, Some(counters.clone()))?;
            write_linear_region(&dest_path, &region, compression_level, Some(counters.clone()))?;
            Ok(())
        }
        ConversionMode::Linear2mca => {
            let region = read_linear_region(&source_path, Some(counters.clone()))?;
            write_anvil_region(&dest_path, &region, compression_level as u32, Some(counters.clone()))?;
            Ok(())
        }
    };

    match result {
        Ok(()) => {
            let dest_size = fs::metadata(&dest_path)?.len();
            let duration = start_time.elapsed();
            
            if verify {
                match mode {
                    ConversionMode::Mca2linear => {
                        linear_region_tools::linear::verify_linear_file(&dest_path);
                    }
                    ConversionMode::Linear2mca => {
                        let _ = read_anvil_region(&dest_path, None)?;
                    }
                }
            }

            stats.add_converted(source_size, dest_size);

            if log_mode {
                let compression_ratio = (dest_size as f64 / source_size as f64) * 100.0;
                println!(
                    "{} -> {} (compression: {:.1}%, time: {:.2}ms)",
                    source_path.display(),
                    dest_path.display(),
                    compression_ratio,
                    duration.as_millis()
                );
            }
        }
        Err(e) => {
            stats.add_error();
            eprintln!("Error converting {}: {}", source_path.display(), e);
            
            let mut current_error = e.source();
            let mut depth = 1;
            while let Some(err) = current_error {
                eprintln!("  Caused by ({}): {}", depth, err);
                current_error = err.source();
                depth += 1;
            }
            if let Ok(metadata) = fs::metadata(&source_path) {
                eprintln!("  File size: {} bytes", metadata.len());
            }
        }
    }

    Ok(())
}

fn find_region_files(dir: &Path, extension: &str) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == extension {
                    files.push(path);
                }
            }
        }
    }
    
    files.sort();
    Ok(files)
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;
    
    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }
    
    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}

fn format_duration(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    let millis = duration.subsec_millis();
    
    if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else if seconds > 0 {
        format!("{}.{:03}s", seconds, millis)
    } else {
        format!("{}ms", duration.as_millis())
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Validate arguments
    if !args.source_dir.exists() {
        anyhow::bail!("Source directory does not exist: {}", args.source_dir.display());
    }

    if !args.source_dir.is_dir() {
        anyhow::bail!("Source path is not a directory: {}", args.source_dir.display());
    }

    // Set up rayon thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.threads)
        .build_global()
        .context("Failed to initialize thread pool")?;

    // Find files to convert
    let file_extension = match args.conversion_mode {
        ConversionMode::Mca2linear => "mca",
        ConversionMode::Linear2mca => "linear",
    };

    let files = find_region_files(&args.source_dir, file_extension)?;
    
    if files.is_empty() {
        println!("No {} files found in {}", file_extension, args.source_dir.display());
        return Ok(());
    }

    println!("Found {} region files to convert", files.len());

    let stats = Arc::new(ConversionStats::new());
    let counters = Arc::new(PerformanceCounters::new());

    let progress_bar = if !args.log {
        let pb = ProgressBar::new(files.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
                .unwrap()
                .progress_chars("#>-"),
        );
        Some(pb)
    } else {
        None
    };

    let start_time = Instant::now();

    files.par_iter().for_each(|source_path| {
        let result = convert_single_file(
            source_path.clone(),
            args.destination_dir.clone(),
            args.conversion_mode.clone(),
            args.compression_level,
            args.skip_existing,
            args.verify,
            stats.clone(),
            counters.clone(),
            args.log,
        );

        if let Err(e) = result {
            eprintln!("Failed to convert {}: {}", source_path.display(), e);
            stats.add_error();
        }

        if let Some(ref pb) = progress_bar {
            pb.inc(1);
        }
    });

    if let Some(pb) = progress_bar {
        pb.finish_with_message("Conversion complete");
    }

    let total_time = start_time.elapsed();

    let converted = stats.converted.load(Ordering::Relaxed);
    let skipped = stats.skipped.load(Ordering::Relaxed);
    let errors = stats.errors.load(Ordering::Relaxed);
    let input_bytes = stats.total_input_bytes.load(Ordering::Relaxed);
    let output_bytes = stats.total_output_bytes.load(Ordering::Relaxed);

    println!("\n=== Conversion Summary ===");
    println!("Files converted: {}", converted);
    println!("Files skipped: {}", skipped);
    println!("Errors: {}", errors);
    println!("Total time: {}", format_duration(total_time));
    
    if converted > 0 {
        println!("Input size: {}", format_bytes(input_bytes));
        println!("Output size: {}", format_bytes(output_bytes));
        println!("Compression ratio: {:.1}%", stats.get_compression_ratio());
        println!("Average speed: {:.1} files/sec", converted as f64 / total_time.as_secs_f64());
        
        let throughput_mb_s = (input_bytes as f64 / (1024.0 * 1024.0)) / total_time.as_secs_f64();
        println!("Throughput: {:.1} MB/s", throughput_mb_s);
    }

    if errors > 0 {
        std::process::exit(1);
    }

    Ok(())
}