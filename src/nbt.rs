use anyhow::{Context, Result};
use std::io::{Read, Write};

pub use fastnbt::{Value, from_bytes, to_bytes, from_reader, to_writer};

#[inline]
pub fn parse_nbt(data: &[u8]) -> Result<Value> {
    from_bytes(data).context("Failed to parse NBT data")
}

#[inline]
pub fn serialize_nbt(value: &Value) -> Result<Vec<u8>> {
    to_bytes(value).context("Failed to serialize NBT data")
}

#[inline]
pub fn parse_nbt_from_reader<R: Read>(reader: R) -> Result<Value> {
    from_reader(reader).context("Failed to parse NBT from reader")
}

#[inline]
pub fn write_nbt_to_writer<W: Write>(writer: W, value: &Value) -> Result<()> {
    to_writer(writer, value).context("Failed to write NBT to writer")
}