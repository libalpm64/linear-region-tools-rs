# linear-region-tools-rs

Linear Regions Tools is the fastest linear converter for Minecraft. It can quickly convert huge worlds from MCA to Linear format (LinearV1 for Foldenor, LinearV2 for Luminol), or from Linear back to MCA.

## Benchmarks

- **Test: 2b2t 100k×100k world download**

![image](https://github.com/user-attachments/assets/9279a103-3873-46c4-a490-8480c7af8207)

- **Test: 2b2t 100k×100k entities**

![image2](https://github.com/user-attachments/assets/aedc2eed-ac8e-448e-a8f3-1a449e3c9a65)


> **Note:** Always convert your MCA files using the intended Minecraft version. If you use a different version and then load the world, you may encounter NBT warnings with entities, because the version tag differs. This causes decoding warnings that will spam your console.  
> 
> If you've already converted the files and accidentally deleted the original MCA files, you can either:
> - Use the NBT Corruption Fixer, or  
> - Convert the files back to MCA format, then load them in a single-player instance—this will automatically upgrade everything to the modern version and remove the errors.  
> 
> The NBT Corruption Fixer is experimental.

---

## NBT Corruption Fixer

### Usage

```sh
fix_nbt_corruption [OPTIONS]
```

### Options

- `-i, --input <INPUT>`
- `-o, --output <OUTPUT>`
- `-f, --format <FORMAT>`        [default: mca]
- `-b, --backup`
- `-t, --threads <THREADS>`      [default: 16]
- `-v, --verbose`
- `-d, --dry-run`                Dry run: do not make changes, but show the output
- `-h, --help`                   Print help

---

## MCA/Linear Converter

### Usage

```sh
./convert_region_files [OPTIONS] <CONVERSION_MODE> <SOURCE_DIR> <DESTINATION_DIR>
```

### Arguments

- `<CONVERSION_MODE>` — `mca2linearv1`, `linearv12mca`, `mca2linearv2`, or `linearv2mca`
- `<SOURCE_DIR>`
- `<DESTINATION_DIR>`

**Conversion Modes:**
- `mca2linearv1` — Convert MCA to LinearV1 (Foldenor format)
- `linearv12mca` — Convert LinearV1 to MCA
- `mca2linearv2` — Convert MCA to LinearV2 (Luminol format)
- `linearv2mca` — Convert LinearV2 to MCA

### Options

- `-t, --threads <THREADS>`               [default: 16]
- `-c, --compression-level <COMPRESSION_LEVEL>` [default: 6]
- `-l, --log`
- `--skip-existing`
- `--verify`
- `-h, --help`

### Build Instructions

```sh
cargo build --release
```

---

## Installing Rust

You can install Rust using `rustup`:

[https://rustup.rs/](https://rustup.rs/)

## Credits
[LeafMC](https://github.com/Winds-Studio/Leaf)
[TriassicLinearPaper](https://github.com/RealTriassic/LinearPaper)
[Xymb-Endcrystal-me](https://github.com/xymb-endcrystalme/LinearRegionFileFormatTools)
[FastNBT](https://github.com/owengage/fastnbt)
