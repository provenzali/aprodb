use std::{
    error::Error,
    fs,
    hint::black_box,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use aprodb::{Config, Database, Durability, Value};
use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use mysql::{Conn, Opts, Params, TxOpts, Value as MyValue, prelude::Queryable};
use postgres::{Client, NoTls, types::ToSql};
use redis::{Commands, Connection as RedisConnection};
use rusqlite::{Connection, params};
use serde::Serialize;

type AnyError = Box<dyn Error + Send + Sync>;
type BenchResult<T> = Result<T, AnyError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum BackendKind {
    Aprodb,
    Sqlite,
    Postgres,
    Mysql,
    Mariadb,
    Redis,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum PayloadProfile {
    Compressible,
    Random,
}

#[derive(Debug, Parser)]
#[command(about = "Reproducible comparative benchmark for AProDB")]
struct Args {
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "aprodb,sqlite,postgres,mysql,mariadb,redis"
    )]
    backends: Vec<BackendKind>,

    #[arg(long, value_delimiter = ',', default_value = "compressible,random")]
    profiles: Vec<PayloadProfile>,

    #[arg(long, default_value_t = 50_000)]
    records: usize,

    #[arg(long, default_value_t = 50_000)]
    reads: usize,

    #[arg(long, default_value_t = 500)]
    payload_bytes: usize,

    #[arg(long, default_value_t = 500)]
    batch_size: usize,

    #[arg(long, default_value_t = 3)]
    runs: usize,

    #[arg(long, default_value_t = 20)]
    scan_repeats: usize,

    #[arg(long, default_value_t = 1_000)]
    scan_limit: usize,

    #[arg(long, default_value = "target/bench-lab/results")]
    workdir: PathBuf,

    #[arg(
        long,
        default_value = "postgresql://postgres@127.0.0.1:55432/aprodb_bench"
    )]
    postgres_url: String,

    #[arg(long, default_value = "mysql://root@127.0.0.1:53306/aprodb_bench")]
    mysql_url: String,

    #[arg(long, default_value = "mysql://root@127.0.0.1:53307/aprodb_bench")]
    mariadb_url: String,

    #[arg(long, default_value = "redis://127.0.0.1:6379/0")]
    redis_url: String,
}

#[derive(Clone)]
struct Record {
    key: String,
    value: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct Report {
    format_version: u32,
    generated_unix_seconds: u64,
    configuration: ReportConfig,
    environment: Environment,
    runs: Vec<RunMetrics>,
    failures: Vec<Failure>,
    notes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ReportConfig {
    backends: Vec<BackendKind>,
    profiles: Vec<PayloadProfile>,
    records: usize,
    reads: usize,
    payload_bytes: usize,
    batch_size: usize,
    repetitions: usize,
    scan_repeats: usize,
    scan_limit: usize,
    durability: String,
}

#[derive(Debug, Serialize)]
struct Environment {
    os: String,
    architecture: String,
    logical_parallelism: usize,
}

#[derive(Debug, Serialize)]
struct RunMetrics {
    backend: BackendKind,
    backend_version: String,
    profile: PayloadProfile,
    repetition: usize,
    ingest_seconds: f64,
    ingest_ops_per_second: f64,
    read_ops_per_second: f64,
    read_latency_us: Latencies,
    scan_ops_per_second: f64,
    scan_latency_us: Latencies,
    physical_data_bytes: u64,
    aprodb_internal: Option<AprodbStorage>,
}

#[derive(Debug, Serialize)]
struct Latencies {
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
}

#[derive(Debug, Serialize)]
struct AprodbStorage {
    logical_value_bytes: u64,
    stored_value_bytes: u64,
    compression_ratio: f64,
    compressed_values: usize,
    raw_values: usize,
    wal_bytes: u64,
}

#[derive(Debug, Serialize)]
struct Failure {
    backend: BackendKind,
    profile: PayloadProfile,
    repetition: usize,
    error: String,
}

trait BenchBackend {
    fn version(&self) -> &str;
    fn reset(&mut self) -> BenchResult<()>;
    fn ingest(&mut self, records: &[Record], batch_size: usize) -> BenchResult<()>;
    fn get_len(&mut self, key: &str) -> BenchResult<Option<usize>>;
    fn scan(&mut self, lower: &str, upper: &str, limit: usize) -> BenchResult<(usize, usize)>;
    fn physical_data_bytes(&mut self) -> BenchResult<u64>;
    fn aprodb_storage(&self) -> Option<AprodbStorage> {
        None
    }
}

fn main() -> BenchResult<()> {
    let args = Args::parse();
    validate_args(&args)?;

    let generated = unix_seconds();
    let session_dir = args.workdir.join(format!("session-{generated}"));
    fs::create_dir_all(&session_dir)?;
    let mut report = Report {
        format_version: 1,
        generated_unix_seconds: generated,
        configuration: ReportConfig {
            backends: args.backends.clone(),
            profiles: args.profiles.clone(),
            records: args.records,
            reads: args.reads,
            payload_bytes: args.payload_bytes,
            batch_size: args.batch_size,
            repetitions: args.runs,
            scan_repeats: args.scan_repeats,
            scan_limit: args.scan_limit,
            durability: "one durable commit per batch; server defaults retained".into(),
        },
        environment: Environment {
            os: std::env::consts::OS.into(),
            architecture: std::env::consts::ARCH.into(),
            logical_parallelism: std::thread::available_parallelism().map_or(1, usize::from),
        },
        runs: Vec::new(),
        failures: Vec::new(),
        notes: vec![
            "AProDB and SQLite run in-process; PostgreSQL, MySQL and MariaDB use one client connection over loopback TCP.".into(),
            "Redis/Valkey uses one loopback TCP connection and a sorted-set range index in addition to the string payloads.".into(),
            "Physical bytes are the AProDB data directory, SQLite database file after checkpoint, SQL table plus indexes, or Redis/Valkey used_memory_dataset; server redo/WAL/AOF files are excluded.".into(),
            "The compressible profile models repetitive document/log fields; random is deterministic high-entropy binary data.".into(),
        ],
    };

    for profile in &args.profiles {
        let records = make_records(args.records, args.payload_bytes, *profile);
        for repetition in 1..=args.runs {
            for backend in &args.backends {
                eprintln!(
                    "running backend={backend:?} profile={profile:?} repetition={repetition}/{}",
                    args.runs
                );
                let run_dir = session_dir
                    .join(format!("{:?}-{:?}-{repetition}", backend, profile).to_ascii_lowercase());
                let result = make_backend(*backend, &run_dir, &args).and_then(|mut instance| {
                    run_workload(
                        &mut *instance,
                        *backend,
                        *profile,
                        repetition,
                        &records,
                        &args,
                    )
                });
                match result {
                    Ok(metrics) => {
                        println!(
                            "{:?}/{:?} run {}: ingest {:.0} ops/s, reads {:.0} ops/s, p99 {:.1} us, {} bytes",
                            backend,
                            profile,
                            repetition,
                            metrics.ingest_ops_per_second,
                            metrics.read_ops_per_second,
                            metrics.read_latency_us.p99,
                            metrics.physical_data_bytes
                        );
                        report.runs.push(metrics);
                    }
                    Err(error) => {
                        eprintln!("FAILED {backend:?}/{profile:?} run {repetition}: {error}");
                        report.failures.push(Failure {
                            backend: *backend,
                            profile: *profile,
                            repetition,
                            error: error.to_string(),
                        });
                    }
                }
                write_report(&session_dir, &report)?;
            }
        }
    }

    let report_path = session_dir.join("report.json");
    println!("report={}", report_path.display());
    if report.failures.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{} benchmark run(s) failed; see report",
            report.failures.len()
        )
        .into())
    }
}

fn validate_args(args: &Args) -> BenchResult<()> {
    if args.records == 0 || args.reads == 0 || args.batch_size == 0 || args.runs == 0 {
        return Err("records, reads, batch-size and runs must be greater than zero".into());
    }
    if args.payload_bytes < 32 {
        return Err("payload-bytes must be at least 32".into());
    }
    if args.scan_repeats == 0 || args.scan_limit == 0 {
        return Err("scan-repeats and scan-limit must be greater than zero".into());
    }
    Ok(())
}

fn make_backend(
    kind: BackendKind,
    run_dir: &Path,
    args: &Args,
) -> BenchResult<Box<dyn BenchBackend>> {
    match kind {
        BackendKind::Aprodb => Ok(Box::new(AprodbBackend::new(run_dir)?)),
        BackendKind::Sqlite => Ok(Box::new(SqliteBackend::new(run_dir)?)),
        BackendKind::Postgres => Ok(Box::new(PostgresBackend::new(&args.postgres_url)?)),
        BackendKind::Mysql => Ok(Box::new(MysqlBackend::new(&args.mysql_url)?)),
        BackendKind::Mariadb => Ok(Box::new(MysqlBackend::new(&args.mariadb_url)?)),
        BackendKind::Redis => Ok(Box::new(RedisBackend::new(&args.redis_url)?)),
    }
}

fn run_workload(
    backend: &mut dyn BenchBackend,
    kind: BackendKind,
    profile: PayloadProfile,
    repetition: usize,
    records: &[Record],
    args: &Args,
) -> BenchResult<RunMetrics> {
    backend.reset()?;
    let start = Instant::now();
    backend.ingest(records, args.batch_size)?;
    let ingest_elapsed = start.elapsed();

    let first = backend.get_len(&records[0].key)?;
    if first != Some(records[0].value.len()) {
        return Err("read-back validation failed after ingest".into());
    }

    let mut read_histogram = Histogram::<u64>::new(3)?;
    let read_start = Instant::now();
    for read_index in 0..args.reads {
        let record_index = deterministic_index(read_index, records.len());
        let item_start = Instant::now();
        let length = backend.get_len(&records[record_index].key)?;
        record_duration(&mut read_histogram, item_start.elapsed())?;
        if length != Some(records[record_index].value.len()) {
            return Err(format!("point-read validation failed at record {record_index}").into());
        }
        black_box(length);
    }
    let read_elapsed = read_start.elapsed();

    let lower = "group:042:";
    let upper = "group:043:";
    let expected = records.len().div_ceil(100).min(args.scan_limit);
    let mut scan_histogram = Histogram::<u64>::new(3)?;
    let scan_start = Instant::now();
    for _ in 0..args.scan_repeats {
        let item_start = Instant::now();
        let (rows, bytes) = backend.scan(lower, upper, args.scan_limit)?;
        record_duration(&mut scan_histogram, item_start.elapsed())?;
        if rows != expected {
            return Err(format!("range-scan returned {rows} rows; expected {expected}").into());
        }
        black_box(bytes);
    }
    let scan_elapsed = scan_start.elapsed();

    let physical_data_bytes = backend.physical_data_bytes()?;
    Ok(RunMetrics {
        backend: kind,
        backend_version: backend.version().to_owned(),
        profile,
        repetition,
        ingest_seconds: ingest_elapsed.as_secs_f64(),
        ingest_ops_per_second: rate(records.len(), ingest_elapsed),
        read_ops_per_second: rate(args.reads, read_elapsed),
        read_latency_us: latencies(&read_histogram),
        scan_ops_per_second: rate(args.scan_repeats, scan_elapsed),
        scan_latency_us: latencies(&scan_histogram),
        physical_data_bytes,
        aprodb_internal: backend.aprodb_storage(),
    })
}

fn make_records(count: usize, payload_bytes: usize, profile: PayloadProfile) -> Vec<Record> {
    (0..count)
        .map(|index| Record {
            key: format!("group:{:03}:item:{index:08}", index % 100),
            value: make_payload(index, payload_bytes, profile),
        })
        .collect()
}

fn make_payload(index: usize, size: usize, profile: PayloadProfile) -> Vec<u8> {
    match profile {
        PayloadProfile::Compressible => {
            let seed = format!(
                "{{\"sensor\":\"temperature\",\"site\":\"rome\",\"status\":\"ok\",\"record\":{index:08}}}\n"
            );
            seed.as_bytes().iter().copied().cycle().take(size).collect()
        }
        PayloadProfile::Random => {
            let mut state = (index as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut output = Vec::with_capacity(size);
            while output.len() < size {
                state = splitmix64(state);
                output.extend_from_slice(&state.to_le_bytes());
            }
            output.truncate(size);
            output
        }
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn deterministic_index(iteration: usize, count: usize) -> usize {
    iteration.wrapping_mul(2_654_435_761) % count
}

fn record_duration(histogram: &mut Histogram<u64>, duration: Duration) -> BenchResult<()> {
    histogram.record(duration.as_nanos().max(1).min(u64::MAX as u128) as u64)?;
    Ok(())
}

fn latencies(histogram: &Histogram<u64>) -> Latencies {
    Latencies {
        p50: histogram.value_at_quantile(0.50) as f64 / 1_000.0,
        p95: histogram.value_at_quantile(0.95) as f64 / 1_000.0,
        p99: histogram.value_at_quantile(0.99) as f64 / 1_000.0,
        max: histogram.max() as f64 / 1_000.0,
    }
}

fn rate(operations: usize, duration: Duration) -> f64 {
    operations as f64 / duration.as_secs_f64()
}

fn write_report(session_dir: &Path, report: &Report) -> BenchResult<()> {
    fs::write(
        session_dir.join("report.json"),
        serde_json::to_vec_pretty(report)?,
    )?;
    Ok(())
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn directory_size(path: &Path) -> BenchResult<u64> {
    let mut bytes = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            bytes = bytes.saturating_add(directory_size(&entry.path())?);
        } else {
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    Ok(bytes)
}

struct AprodbBackend {
    database: Database,
    path: PathBuf,
    version: String,
}

impl AprodbBackend {
    fn new(path: &Path) -> BenchResult<Self> {
        let mut config = Config::new(path);
        config.durability = Durability::SyncData;
        let database = Database::open(config)?;
        Ok(Self {
            database,
            path: path.to_owned(),
            version: format!("AProDB {}", env!("CARGO_PKG_VERSION")),
        })
    }
}

impl BenchBackend for AprodbBackend {
    fn version(&self) -> &str {
        &self.version
    }

    fn reset(&mut self) -> BenchResult<()> {
        Ok(())
    }

    fn ingest(&mut self, records: &[Record], batch_size: usize) -> BenchResult<()> {
        for chunk in records.chunks(batch_size) {
            let entries = chunk
                .iter()
                .map(|record| (record.key.clone(), Value::Bytes(record.value.clone())))
                .collect();
            self.database.put_batch(entries)?;
        }
        Ok(())
    }

    fn get_len(&mut self, key: &str) -> BenchResult<Option<usize>> {
        match self.database.get(key)? {
            Some(Value::Bytes(value)) => Ok(Some(value.len())),
            None => Ok(None),
            Some(_) => Err("AProDB returned an unexpected value type".into()),
        }
    }

    fn scan(&mut self, lower: &str, _upper: &str, limit: usize) -> BenchResult<(usize, usize)> {
        let rows = self.database.scan_prefix(lower, limit)?;
        let bytes = rows
            .iter()
            .map(|(_, value)| match value {
                Value::Bytes(bytes) => bytes.len(),
                _ => 0,
            })
            .sum();
        Ok((rows.len(), bytes))
    }

    fn physical_data_bytes(&mut self) -> BenchResult<u64> {
        self.database.sync()?;
        directory_size(&self.path)
    }

    fn aprodb_storage(&self) -> Option<AprodbStorage> {
        self.database.stats().ok().map(|stats| AprodbStorage {
            logical_value_bytes: stats.logical_value_bytes,
            stored_value_bytes: stats.stored_value_bytes,
            compression_ratio: stats.compression_ratio,
            compressed_values: stats.compressed_values,
            raw_values: stats.raw_values,
            wal_bytes: stats.wal_bytes,
        })
    }
}

struct SqliteBackend {
    connection: Connection,
    path: PathBuf,
    version: String,
}

impl SqliteBackend {
    fn new(run_dir: &Path) -> BenchResult<Self> {
        fs::create_dir_all(run_dir)?;
        let path = run_dir.join("benchmark.sqlite3");
        let connection = Connection::open(&path)?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA temp_store=MEMORY;",
        )?;
        let version = connection.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
        Ok(Self {
            connection,
            path,
            version,
        })
    }
}

impl BenchBackend for SqliteBackend {
    fn version(&self) -> &str {
        &self.version
    }

    fn reset(&mut self) -> BenchResult<()> {
        self.connection.execute_batch(
            "DROP TABLE IF EXISTS bench_kv;
             CREATE TABLE bench_kv (k TEXT PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID;",
        )?;
        Ok(())
    }

    fn ingest(&mut self, records: &[Record], batch_size: usize) -> BenchResult<()> {
        for chunk in records.chunks(batch_size) {
            let transaction = self.connection.transaction()?;
            {
                let mut statement =
                    transaction.prepare("INSERT INTO bench_kv(k, v) VALUES (?1, ?2)")?;
                for record in chunk {
                    statement.execute(params![record.key, record.value])?;
                }
            }
            transaction.commit()?;
        }
        Ok(())
    }

    fn get_len(&mut self, key: &str) -> BenchResult<Option<usize>> {
        let mut statement = self
            .connection
            .prepare_cached("SELECT length(v) FROM bench_kv WHERE k=?1")?;
        let mut rows = statement.query([key])?;
        Ok(rows
            .next()?
            .map(|row| row.get::<_, i64>(0))
            .transpose()?
            .map(|value| value as usize))
    }

    fn scan(&mut self, lower: &str, upper: &str, limit: usize) -> BenchResult<(usize, usize)> {
        let mut statement = self.connection.prepare_cached(
            "SELECT length(v) FROM bench_kv WHERE k >= ?1 AND k < ?2 ORDER BY k LIMIT ?3",
        )?;
        let lengths = statement.query_map(params![lower, upper, limit as i64], |row| {
            row.get::<_, i64>(0)
        })?;
        let mut rows = 0;
        let mut bytes = 0;
        for length in lengths {
            rows += 1;
            bytes += length? as usize;
        }
        Ok((rows, bytes))
    }

    fn physical_data_bytes(&mut self) -> BenchResult<u64> {
        self.connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok(fs::metadata(&self.path)?.len())
    }
}

struct PostgresBackend {
    client: Client,
    version: String,
}

impl PostgresBackend {
    fn new(url: &str) -> BenchResult<Self> {
        let mut client = Client::connect(url, NoTls)?;
        let version: String = client.query_one("SHOW server_version", &[])?.get(0);
        Ok(Self { client, version })
    }

    fn insert_sql(rows: usize) -> String {
        let values = (0..rows)
            .map(|index| format!("(${}, ${})", index * 2 + 1, index * 2 + 2))
            .collect::<Vec<_>>()
            .join(",");
        format!("INSERT INTO bench_kv(k, v) VALUES {values}")
    }
}

impl BenchBackend for PostgresBackend {
    fn version(&self) -> &str {
        &self.version
    }

    fn reset(&mut self) -> BenchResult<()> {
        self.client.batch_execute(
            "DROP TABLE IF EXISTS bench_kv;
             CREATE TABLE bench_kv (k TEXT PRIMARY KEY, v BYTEA NOT NULL);",
        )?;
        Ok(())
    }

    fn ingest(&mut self, records: &[Record], batch_size: usize) -> BenchResult<()> {
        for chunk in records.chunks(batch_size) {
            let sql = Self::insert_sql(chunk.len());
            let mut transaction = self.client.transaction()?;
            let parameters: Vec<&(dyn ToSql + Sync)> = chunk
                .iter()
                .flat_map(|record| {
                    [
                        &record.key as &(dyn ToSql + Sync),
                        &record.value as &(dyn ToSql + Sync),
                    ]
                })
                .collect();
            transaction.execute(&sql, &parameters)?;
            transaction.commit()?;
        }
        Ok(())
    }

    fn get_len(&mut self, key: &str) -> BenchResult<Option<usize>> {
        let row = self
            .client
            .query_opt("SELECT octet_length(v) FROM bench_kv WHERE k=$1", &[&key])?;
        Ok(row.map(|row| row.get::<_, i32>(0) as usize))
    }

    fn scan(&mut self, lower: &str, upper: &str, limit: usize) -> BenchResult<(usize, usize)> {
        let rows = self.client.query(
            "SELECT octet_length(v) FROM bench_kv WHERE k >= $1 AND k < $2 ORDER BY k LIMIT $3",
            &[&lower, &upper, &(limit as i64)],
        )?;
        let bytes = rows.iter().map(|row| row.get::<_, i32>(0) as usize).sum();
        Ok((rows.len(), bytes))
    }

    fn physical_data_bytes(&mut self) -> BenchResult<u64> {
        let bytes: i64 = self
            .client
            .query_one("SELECT pg_total_relation_size('bench_kv')::bigint", &[])?
            .get(0);
        Ok(bytes as u64)
    }
}

struct MysqlBackend {
    connection: Conn,
    version: String,
}

impl MysqlBackend {
    fn new(url: &str) -> BenchResult<Self> {
        let options = Opts::from_url(url)?;
        let mut connection = Conn::new(options)?;
        let version = connection
            .query_first::<String, _>("SELECT VERSION()")?
            .unwrap_or_else(|| "unknown".into());
        Ok(Self {
            connection,
            version,
        })
    }

    fn insert_sql(rows: usize) -> String {
        format!(
            "INSERT INTO bench_kv(k, v) VALUES {}",
            std::iter::repeat_n("(?, ?)", rows)
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

impl BenchBackend for MysqlBackend {
    fn version(&self) -> &str {
        &self.version
    }

    fn reset(&mut self) -> BenchResult<()> {
        self.connection
            .query_drop("DROP TABLE IF EXISTS bench_kv")?;
        self.connection.query_drop(
            "CREATE TABLE bench_kv (
                k VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
                v LONGBLOB NOT NULL
             ) ENGINE=InnoDB",
        )?;
        Ok(())
    }

    fn ingest(&mut self, records: &[Record], batch_size: usize) -> BenchResult<()> {
        for chunk in records.chunks(batch_size) {
            let sql = Self::insert_sql(chunk.len());
            let values = chunk
                .iter()
                .flat_map(|record| {
                    [
                        MyValue::Bytes(record.key.as_bytes().to_vec()),
                        MyValue::Bytes(record.value.clone()),
                    ]
                })
                .collect();
            let mut transaction = self.connection.start_transaction(TxOpts::default())?;
            transaction.exec_drop(sql, Params::Positional(values))?;
            transaction.commit()?;
        }
        Ok(())
    }

    fn get_len(&mut self, key: &str) -> BenchResult<Option<usize>> {
        let length = self
            .connection
            .exec_first::<u64, _, _>("SELECT OCTET_LENGTH(v) FROM bench_kv WHERE k=?", (key,))?;
        Ok(length.map(|value| value as usize))
    }

    fn scan(&mut self, lower: &str, upper: &str, limit: usize) -> BenchResult<(usize, usize)> {
        let lengths = self.connection.exec::<u64, _, _>(
            "SELECT OCTET_LENGTH(v) FROM bench_kv WHERE k >= ? AND k < ? ORDER BY k LIMIT ?",
            (lower, upper, limit as u64),
        )?;
        let bytes = lengths.iter().map(|value| *value as usize).sum();
        Ok((lengths.len(), bytes))
    }

    fn physical_data_bytes(&mut self) -> BenchResult<u64> {
        // TABLES metrics are available to ordinary benchmark users; querying
        // InnoDB tablespace metadata would require the PROCESS privilege.
        let bytes = self.connection.query_first::<(Option<u64>, Option<u64>), _>(
            "SELECT DATA_LENGTH, INDEX_LENGTH FROM information_schema.TABLES
             WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='bench_kv'",
        )?;
        Ok(bytes.map(|(data, index)| data.unwrap_or(0) + index.unwrap_or(0)).unwrap_or(0))
    }
}

struct RedisBackend {
    connection: RedisConnection,
    version: String,
}

impl RedisBackend {
    fn new(url: &str) -> BenchResult<Self> {
        let client = redis::Client::open(url)?;
        let mut connection = client.get_connection()?;
        let info: String = redis::cmd("INFO").arg("server").query(&mut connection)?;
        let version = parse_info_value(&info, "redis_version")
            .or_else(|| parse_info_value(&info, "valkey_version"))
            .unwrap_or_else(|| "unknown".into());
        Ok(Self {
            connection,
            version,
        })
    }
}

impl BenchBackend for RedisBackend {
    fn version(&self) -> &str {
        &self.version
    }

    fn reset(&mut self) -> BenchResult<()> {
        redis::cmd("FLUSHDB").query::<()>(&mut self.connection)?;
        Ok(())
    }

    fn ingest(&mut self, records: &[Record], batch_size: usize) -> BenchResult<()> {
        for chunk in records.chunks(batch_size) {
            let mut pipe = redis::pipe();
            for record in chunk {
                pipe.cmd("SET").arg(&record.key).arg(&record.value);
                pipe.cmd("ZADD").arg("bench_index").arg(0).arg(&record.key);
            }
            pipe.query::<()>(&mut self.connection)?;
        }
        Ok(())
    }

    fn get_len(&mut self, key: &str) -> BenchResult<Option<usize>> {
        let exists: bool = self.connection.exists(key)?;
        if !exists {
            return Ok(None);
        }
        let length: usize = redis::cmd("STRLEN").arg(key).query(&mut self.connection)?;
        Ok(Some(length))
    }

    fn scan(&mut self, lower: &str, upper: &str, limit: usize) -> BenchResult<(usize, usize)> {
        let keys: Vec<String> = redis::cmd("ZRANGEBYLEX")
            .arg("bench_index")
            .arg(format!("[{lower}"))
            .arg(format!("({upper}"))
            .arg("LIMIT")
            .arg(0)
            .arg(limit)
            .query(&mut self.connection)?;
        let mut pipe = redis::pipe();
        for key in &keys {
            pipe.cmd("STRLEN").arg(key);
        }
        let lengths: Vec<usize> = pipe.query(&mut self.connection)?;
        Ok((keys.len(), lengths.iter().sum()))
    }

    fn physical_data_bytes(&mut self) -> BenchResult<u64> {
        let info: String = redis::cmd("INFO").arg("memory").query(&mut self.connection)?;
        Ok(parse_info_value(&info, "used_memory_dataset")
            .or_else(|| parse_info_value(&info, "used_memory"))
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0))
    }
}

fn parse_info_value(info: &str, key: &str) -> Option<String> {
    info.lines()
        .filter_map(|line| line.split_once(':'))
        .find_map(|(name, value)| (name == key).then(|| value.trim().to_owned()))
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
