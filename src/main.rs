use std::{path::PathBuf, str::FromStr, time::Instant};

use aprodb::{ComputeBackend, Config, Database, Durability, Metric, Value};
use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;

#[derive(Parser)]
#[command(
    name = "aprodb",
    version,
    about = "Database parallelo key-value e vettoriale"
)]
struct Cli {
    #[arg(long, global = true, default_value = ".aprodb")]
    path: PathBuf,

    #[arg(
        long,
        global = true,
        help = "Riduce la durabilità per aumentare il throughput"
    )]
    relaxed: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Put {
        key: String,
        value: String,
        #[arg(long, value_enum, default_value_t = ValueKind::Text)]
        kind: ValueKind,
    },
    Get {
        key: String,
    },
    Delete {
        key: String,
    },
    Scan {
        #[arg(default_value = "")]
        prefix: String,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    VectorSearch {
        #[arg(help = "Componenti separate da virgola, es. 0.2,0.5,0.1")]
        query: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long, value_enum, default_value_t = CliMetric::Cosine)]
        metric: CliMetric,
        #[arg(long, value_enum, default_value_t = CliBackend::Auto)]
        backend: CliBackend,
    },
    Stats,
    Snapshot,
    GpuInfo,
    Demo {
        #[arg(long, default_value_t = 10_000)]
        items: usize,
        #[arg(long, default_value_t = 128)]
        dimension: usize,
        #[arg(long, value_enum, default_value_t = CliBackend::Auto)]
        backend: CliBackend,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum ValueKind {
    Text,
    Bytes,
    Integer,
    Float,
    Vector,
}

#[derive(Clone, Copy, ValueEnum)]
enum CliMetric {
    Dot,
    Cosine,
}

#[derive(Clone, Copy, ValueEnum)]
enum CliBackend {
    Auto,
    Cpu,
    Gpu,
}

#[derive(Serialize)]
struct PutOutput {
    key: String,
    sequence: u64,
}

#[derive(Serialize)]
struct DeleteOutput {
    key: String,
    deleted: bool,
}

#[derive(Serialize)]
struct SnapshotOutput {
    records: usize,
}

#[derive(Serialize)]
struct GpuOutput {
    adapter: String,
}

#[derive(Serialize)]
struct DemoOutput {
    items: usize,
    dimension: usize,
    ingest_ms: u128,
    ingest_ops_per_second: f64,
    search_ms: u128,
    result: aprodb::SearchResult,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let mut config = Config::new(&cli.path);
    if cli.relaxed {
        config.durability = Durability::Relaxed;
    }
    let database = Database::open(config)?;

    match cli.command {
        Command::Put { key, value, kind } => {
            let parsed = parse_value(kind, &value)?;
            let sequence = database.put(key.clone(), parsed)?;
            print_json(&PutOutput { key, sequence })?;
        }
        Command::Get { key } => print_json(&database.get(&key)?)?,
        Command::Delete { key } => {
            let deleted = database.delete(&key)?;
            print_json(&DeleteOutput { key, deleted })?;
        }
        Command::Scan { prefix, limit } => print_json(&database.scan_prefix(&prefix, limit)?)?,
        Command::VectorSearch {
            query,
            limit,
            metric,
            backend,
        } => {
            let query = parse_vector(&query)?;
            let result = database.vector_search(&query, limit, metric.into(), backend.into())?;
            print_json(&result)?;
        }
        Command::Stats => print_json(&database.stats()?)?,
        Command::Snapshot => print_json(&SnapshotOutput {
            records: database.snapshot()?,
        })?,
        Command::GpuInfo => print_json(&GpuOutput {
            adapter: database.initialize_gpu()?,
        })?,
        Command::Demo {
            items,
            dimension,
            backend,
        } => run_demo(&database, items, dimension, backend.into())?,
    }
    Ok(())
}

fn parse_value(kind: ValueKind, input: &str) -> Result<Value, Box<dyn std::error::Error>> {
    Ok(match kind {
        ValueKind::Text => Value::Text(input.to_owned()),
        ValueKind::Bytes => Value::Bytes(parse_hex(input)?),
        ValueKind::Integer => Value::Integer(i64::from_str(input)?),
        ValueKind::Float => Value::Float(f64::from_str(input)?),
        ValueKind::Vector => Value::Vector(parse_vector(input)?),
    })
}

fn parse_vector(input: &str) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
    if input.trim().is_empty() {
        return Err("il vettore non può essere vuoto".into());
    }
    input
        .split(',')
        .map(|part| Ok(f32::from_str(part.trim())?))
        .collect()
}

fn parse_hex(input: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let bytes = input.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err("una stringa esadecimale deve avere lunghezza pari".into());
    }
    bytes
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair)?;
            Ok(u8::from_str_radix(text, 16)?)
        })
        .collect()
}

fn run_demo(
    database: &Database,
    items: usize,
    dimension: usize,
    backend: ComputeBackend,
) -> Result<(), Box<dyn std::error::Error>> {
    if items == 0 || dimension == 0 {
        return Err("items e dimension devono essere maggiori di zero".into());
    }
    let batch: Vec<_> = (0..items)
        .map(|row| {
            let vector = (0..dimension)
                .map(|column| deterministic_component(row, column))
                .collect();
            (format!("demo:vector:{row:08}"), Value::Vector(vector))
        })
        .collect();
    let ingest_start = Instant::now();
    database.put_batch(batch)?;
    let ingest = ingest_start.elapsed();
    let query: Vec<_> = (0..dimension)
        .map(|column| deterministic_component(items / 2, column))
        .collect();
    let search_start = Instant::now();
    let result = database.vector_search(&query, 5, Metric::Cosine, backend)?;
    let search = search_start.elapsed();
    let output = DemoOutput {
        items,
        dimension,
        ingest_ms: ingest.as_millis(),
        ingest_ops_per_second: items as f64 / ingest.as_secs_f64(),
        search_ms: search.as_millis(),
        result,
    };
    print_json(&output)?;
    Ok(())
}

fn deterministic_component(row: usize, column: usize) -> f32 {
    let mixed = row
        .wrapping_mul(1_664_525)
        .wrapping_add(column.wrapping_mul(1_013_904_223))
        .wrapping_add(1_013_904_223);
    (mixed % 10_000) as f32 / 10_000.0
}

fn print_json(value: &impl Serialize) -> Result<(), serde_json::Error> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

impl From<CliMetric> for Metric {
    fn from(value: CliMetric) -> Self {
        match value {
            CliMetric::Dot => Self::Dot,
            CliMetric::Cosine => Self::Cosine,
        }
    }
}

impl From<CliBackend> for ComputeBackend {
    fn from(value: CliBackend) -> Self {
        match value {
            CliBackend::Auto => Self::Auto,
            CliBackend::Cpu => Self::Cpu,
            CliBackend::Gpu => Self::Gpu,
        }
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
