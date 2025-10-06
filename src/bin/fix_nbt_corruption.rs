use anyhow::{Context, Result};
use clap::Parser;
use fastnbt::Value;
use indicatif::{ProgressBar, ProgressStyle};
use linear_region_tools::{
    anvil::{read_anvil_region, write_anvil_region},
    linear::{read_linear_region, write_linear_region},
    Chunk, Region,
};
use rayon::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "fix_nbt_corruption")]
#[command(about = "Fix NBT corruption issues in Minecraft region files")]
struct Args {
    #[arg(short, long)]
    input: PathBuf,

    #[arg(short, long)]
    output: Option<PathBuf>,

    #[arg(short, long, default_value = "mca")]
    format: String,

    #[arg(short, long, default_value_t = false)]
    backup: bool,

    #[arg(short, long, default_value_t = num_cpus::get())]
    threads: usize,

    #[arg(short, long)]
    verbose: bool,

    /// Dry run, do not make changes but see the output.
    #[arg(short, long)]
    dry_run: bool,
}

#[derive(Debug, Default)]
struct FixStats {
    files_processed: usize,
    chunks_fixed: usize,
    entities_fixed: usize,
    enchantments_fixed: usize,
    uuids_regenerated: usize,
    positions_fixed: usize,
}

impl FixStats {
    fn merge(&mut self, other: &FixStats) {
        self.files_processed += other.files_processed;
        self.chunks_fixed += other.chunks_fixed;
        self.entities_fixed += other.entities_fixed;
        self.enchantments_fixed += other.enchantments_fixed;
        self.uuids_regenerated += other.uuids_regenerated;
        self.positions_fixed += other.positions_fixed;
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    rayon::ThreadPoolBuilder::new()
        .num_threads(args.threads)
        .build_global()
        .context("Failed to initialize thread pool")?;

    let extension = match args.format.as_str() {
        "mca" => "mca",
        "linear" => "linear",
        _ => return Err(anyhow::anyhow!("Invalid format: {}", args.format)),
    };
    
    let mut files = Vec::new();
    for entry in fs::read_dir(&args.input)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().map_or(false, |ext| ext == extension) {
            files.push(path);
        }
    }
    files.sort();
    
    if files.is_empty() {
        println!("No {} files found in {}", args.format, args.input.display());
        return Ok(());
    }

    println!("Found {} {} files to process", files.len(), args.format);
    
    if args.dry_run {
        println!("DRY RUN MODE - No files will be modified");
    }

    let progress = ProgressBar::new(files.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .unwrap()
    );

    let total_stats = files
        .par_iter()
        .map(|file_path| {
            let result = fix_region_file(file_path, &args);
            progress.inc(1);
            
            match result {
                Ok(stats) => {
                    if args.verbose {
                        progress.println(format!("Fixed {}: {} entities, {} enchantments", 
                            file_path.display(), stats.entities_fixed, stats.enchantments_fixed));
                    }
                    stats
                }
                Err(e) => {
                    progress.println(format!("Error processing {}: {}", file_path.display(), e));
                    FixStats::default()
                }
            }
        })
        .reduce(|| FixStats::default(), |mut acc, stats| {
            acc.merge(&stats);
            acc
        });

    progress.finish_with_message("Complete!");

    println!("\n=== Fix Summary ===");
    println!("Files processed: {}", total_stats.files_processed);
    println!("Chunks fixed: {}", total_stats.chunks_fixed);
    println!("Entities fixed: {}", total_stats.entities_fixed);
    println!("Enchantments fixed: {}", total_stats.enchantments_fixed);
    println!("UUIDs regenerated: {}", total_stats.uuids_regenerated);
    println!("Positions fixed: {}", total_stats.positions_fixed);

    Ok(())
}

fn fix_region_file(file_path: &Path, args: &Args) -> Result<FixStats> {
    let mut stats = FixStats::default();
    stats.files_processed = 1;
    
    if args.backup && !args.dry_run {
        let backup_path = file_path.with_extension(format!("{}.backup", 
            file_path.extension().unwrap().to_str().unwrap()));
        fs::copy(file_path, backup_path)?;
    }

    let mut region = match args.format.as_str() {
        "mca" => read_anvil_region(file_path, None)?,
        "linear" => read_linear_region(file_path, None)?,
        _ => return Err(anyhow::anyhow!("Invalid format: {}", args.format)),
    };

    let mut region_modified = false;
    let mut used_uuids = HashSet::new();

    for chunk in region.chunks.values_mut() {
        let chunk_stats = fix_chunk(chunk, &mut used_uuids)?;
        
        if chunk_stats.entities_fixed > 0 || chunk_stats.enchantments_fixed > 0 || 
           chunk_stats.uuids_regenerated > 0 || chunk_stats.positions_fixed > 0 {
            region_modified = true;
            stats.chunks_fixed += 1;
        }
        
        stats.merge(&chunk_stats);
    }

    if region_modified && !args.dry_run {
        let output_path = if let Some(output_dir) = &args.output {
            output_dir.join(file_path.file_name().unwrap())
        } else {
            file_path.to_path_buf()
        };

        match args.format.as_str() {
            "mca" => write_anvil_region(&output_path, &region, 6, None)?,
            "linear" => write_linear_region(&output_path, &region, 3, None)?,
            _ => unreachable!(),
        }
    }

    Ok(stats)
}

fn should_delete_entity(entity: &Value) -> bool {
    let Value::Compound(entity_data) = entity else { return false };
    
    let has_custom_data = |item: &Value| {
        if let Value::Compound(item_data) = item {
            if let Some(Value::Compound(components)) = item_data.get("components") {
                return components.contains_key("minecraft:custom_data");
            }
        }
        false
    };

    if let Some(Value::Compound(equipment)) = entity_data.get("equipment") {
        if equipment.values().any(has_custom_data) { return true; }
    }
    
    for field in ["ArmorItems", "HandItems"] {
        if let Some(Value::List(items)) = entity_data.get(field) {
            if items.iter().any(has_custom_data) { return true; }
        }
    }
    
    false
}

fn fix_chunk(chunk: &mut Chunk, used_uuids: &mut HashSet<String>) -> Result<FixStats> {
    let mut stats = FixStats::default();

    let mut nbt = chunk.parse_nbt()?;
    let mut modified = false;

    if let Value::Compound(compound) = &mut nbt {
        for entities_field in ["Entities", "entities"] {
            if let Some(Value::List(entities)) = compound.get_mut(entities_field) {
                let original_count = entities.len();
                entities.retain(|entity| !should_delete_entity(entity));
                
                let deleted_count = original_count - entities.len();
                if deleted_count > 0 {
                    stats.entities_fixed += deleted_count;
                    modified = true;
                }
                
                for entity in entities {
                    let entity_stats = fix_entity(entity, chunk.x, chunk.z, used_uuids)?;
                    if entity_stats.entities_fixed > 0 || entity_stats.enchantments_fixed > 0 || 
                       entity_stats.uuids_regenerated > 0 || entity_stats.positions_fixed > 0 {
                        modified = true;
                    }
                    stats.merge(&entity_stats);
                }
            }
        }
    }

    if modified {
        *chunk = Chunk::from_nbt(&nbt, chunk.x, chunk.z)?;
    }

    Ok(stats)
}

fn fix_entity(entity: &mut Value, chunk_x: i32, chunk_z: i32, used_uuids: &mut HashSet<String>) -> Result<FixStats> {
    let mut stats = FixStats::default();

    if let Value::Compound(entity_data) = entity {
        let mut entity_modified = false;

        for field in ["equipment", "ArmorItems", "HandItems"] {
            if let Some(items) = entity_data.get_mut(field) {
                let enchant_stats = fix_items_enchantments(items)?;
                stats.merge(&enchant_stats);
                if enchant_stats.enchantments_fixed > 0 {
                    entity_modified = true;
                }
            }
        }

        if let Some(item) = entity_data.get_mut("Item") {
            let item_stats = fix_item_enchantments(item)?;
            stats.merge(&item_stats);
            if item_stats.enchantments_fixed > 0 {
                entity_modified = true;
            }
        }

        if let Some(uuid_value) = entity_data.get_mut("UUID") {
            let uuid_stats = fix_entity_uuid(uuid_value, used_uuids)?;
            stats.merge(&uuid_stats);
            if uuid_stats.uuids_regenerated > 0 {
                entity_modified = true;
            }
        }

        if let Some(pos) = entity_data.get_mut("Pos") {
            let pos_stats = fix_entity_position(pos, chunk_x, chunk_z)?;
            stats.merge(&pos_stats);
            if pos_stats.positions_fixed > 0 {
                entity_modified = true;
            }
        }

        if let Some(Value::List(passengers)) = entity_data.get_mut("Passengers") {
            for passenger in passengers {
                let passenger_stats = fix_entity(passenger, chunk_x, chunk_z, used_uuids)?;
                stats.merge(&passenger_stats);
                if passenger_stats.entities_fixed > 0 || passenger_stats.enchantments_fixed > 0 || 
                   passenger_stats.uuids_regenerated > 0 || passenger_stats.positions_fixed > 0 {
                    entity_modified = true;
                }
            }
        }

        if entity_modified {
            stats.entities_fixed += 1;
        }
    }

    Ok(stats)
}

fn fix_items_enchantments(items: &mut Value) -> Result<FixStats> {
    let mut stats = FixStats::default();

    match items {
        Value::Compound(eq_data) => {
            for slot in ["head", "chest", "legs", "feet", "mainhand", "offhand"] {
                if let Some(item) = eq_data.get_mut(slot) {
                    let item_stats = fix_item_enchantments(item)?;
                    stats.merge(&item_stats);
                }
            }
        }
        Value::List(items_list) => {
            for item in items_list {
                let item_stats = fix_item_enchantments(item)?;
                stats.merge(&item_stats);
            }
        }
        _ => {}
    }

    Ok(stats)
}

fn fix_item_enchantments(item: &mut Value) -> Result<FixStats> {
    let mut stats = FixStats::default();

    if let Value::Compound(item_data) = item {
        if let Some(Value::Compound(components)) = item_data.get_mut("components") {
            if let Some(enchants) = components.get_mut("minecraft:enchantments") {
                if let Value::Compound(enchant_map) = enchants {
                    if let Some(Value::Compound(levels)) = enchant_map.get_mut("levels") {
                        stats.enchantments_fixed += fix_enchantment_levels(levels);
                    } else {
                        stats.enchantments_fixed += fix_enchantment_levels(enchant_map);
                    }
                }
            }

            if let Some(Value::Compound(custom_data)) = components.get_mut("minecraft:custom_data") {
                if custom_data.remove("VV|Protocol1_20_3To1_20_5").is_some() {
                    stats.enchantments_fixed += 1;
                }
                
                if let Some(Value::List(enchantments)) = custom_data.get_mut("Enchantments") {
                    for enchant in enchantments {
                        if let Value::Compound(enchant_data) = enchant {
                            if let Some(Value::Short(lvl)) = enchant_data.get_mut("lvl") {
                                if *lvl == 0 {
                                    *lvl = 1;
                                    stats.enchantments_fixed += 1;
                                }
                            }
                        }
                    }
                }
            }
        }

        if let Some(Value::List(enchantments)) = item_data.get_mut("Enchantments") {
            for enchant in enchantments {
                if let Value::Compound(enchant_data) = enchant {
                    if let Some(Value::Short(lvl)) = enchant_data.get_mut("lvl") {
                        if *lvl == 0 {
                            *lvl = 1;
                            stats.enchantments_fixed += 1;
                        }
                    }
                }
            }
        }
    }

    Ok(stats)
}

fn fix_enchantment_levels(enchant_map: &mut HashMap<String, Value>) -> usize {
    let mut fixed_count = 0;
    
    for (_enchant_name, level) in enchant_map.iter_mut() {
        match level {
            Value::Int(lvl) => {
                if *lvl == 0 {
                    *lvl = 1;
                    fixed_count += 1;
                }
            }
            Value::Short(lvl) => {
                if *lvl == 0 {
                    *lvl = 1;
                    fixed_count += 1;
                }
            }
            Value::Byte(lvl) => {
                if *lvl == 0 {
                    *lvl = 1;
                    fixed_count += 1;
                }
            }
            _ => {
                // Unhandled type
            }
        }
    }
    
    fixed_count
}

fn fix_entity_uuid(uuid_value: &mut Value, used_uuids: &mut HashSet<String>) -> Result<FixStats> {
    let mut stats = FixStats::default();

    let uuid_str = match uuid_value {
        Value::String(s) => s.clone(),
        Value::IntArray(arr) if arr.len() == 4 => {
            let uuid = Uuid::from_u128(
                ((arr[0] as u128) << 96) |
                ((arr[1] as u128) << 64) |
                ((arr[2] as u128) << 32) |
                (arr[3] as u128)
            );
            uuid.to_string()
        }
        _ => return Ok(stats),
    };

    if used_uuids.contains(&uuid_str) {
        let new_uuid = Uuid::new_v4();
        let new_uuid_str = new_uuid.to_string();
        
        match uuid_value {
            Value::String(s) => *s = new_uuid_str.clone(),
            Value::IntArray(arr) => {
                let uuid_u128 = new_uuid.as_u128();
                arr[0] = (uuid_u128 >> 96) as i32;
                arr[1] = (uuid_u128 >> 64) as i32;
                arr[2] = (uuid_u128 >> 32) as i32;
                arr[3] = uuid_u128 as i32;
            }
            _ => {}
        }
        
        used_uuids.insert(new_uuid_str);
        stats.uuids_regenerated += 1;
    } else {
        used_uuids.insert(uuid_str);
    }

    Ok(stats)
}

fn fix_entity_position(pos: &mut Value, chunk_x: i32, chunk_z: i32) -> Result<FixStats> {
    let mut stats = FixStats::default();

    if let Value::List(coords) = pos {
        if coords.len() >= 3 {
            let mut position_fixed = false;
            let expected_min_x = (chunk_x * 16) as f64;
            let expected_max_x = ((chunk_x + 1) * 16) as f64;
            let expected_min_z = (chunk_z * 16) as f64;
            let expected_max_z = ((chunk_z + 1) * 16) as f64;

            if let Value::Double(x) = &coords[0] {
                if *x < expected_min_x || *x >= expected_max_x {
                    coords[0] = Value::Double(expected_min_x + 8.0);
                    position_fixed = true;
                }
            }

            if let Value::Double(z) = &coords[2] {
                if *z < expected_min_z || *z >= expected_max_z {
                    coords[2] = Value::Double(expected_min_z + 8.0);
                    position_fixed = true;
                }
            }

            if position_fixed {
                stats.positions_fixed += 1;
            }
        }
    }

    Ok(stats)
}