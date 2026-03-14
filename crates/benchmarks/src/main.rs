use criterion::{Criterion, Throughput};
use dsl::{get, update};
use minicbor as cbor;
use minicbor::data::Type;
use minicbor::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};
use store::backend::memory_backend::MemoryBackend;
use store::backend::sled_backend::SledBackend;
use store::backend::{
    fjall_backend::FjallBackend, rocksdb_backend::RocksDbBackend,
    surrealkv_backend::SurrealKvBackend, tidehunter_backend::TidehunterBackend,
    turso_backend::TursoBackend, StorageBackend,
};
use store::{Store, StoreError};
use sysinfo::{ProcessesToUpdate, System};

type PoolId = [u8; 28];
type CertificatePointer = (u64, u64, u64);
type DRep = [u8; 28];
type Lovelace = u64;

#[derive(Serialize, Deserialize)]
struct BenchmarkReport {
    config: ReportConfig,
    results: Vec<BackendReport>,
    generated_at_unix_ms: u128,
}

#[derive(Serialize, Deserialize)]
struct ReportConfig {
    entries: usize,
    key_size: usize,
    value_size: usize,
    namespaces: usize,
    subset_size: usize,
}

#[derive(Serialize, Deserialize)]
struct BackendReport {
    backend: String,
    on_disk_bytes: Option<u64>,
    memory: MemoryProfile,
    insert: Metric,
    full_read: Metric,
    partial_get_static: Metric,
    full_update_static: Metric,
    partial_update_static: Metric,
    delete: Metric,
}

#[derive(Serialize, Deserialize)]
struct Metric {
    operations: usize,
    elapsed_ms: u128,
    elapsed_us: u128,
    ops_per_sec: f64,
}

#[derive(Serialize, Deserialize)]
struct MemoryProfile {
    rss_before_bytes: u64,
    rss_peak_bytes: u64,
    rss_after_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Row {
    pub pool: Option<(PoolId, CertificatePointer)>,
    pub deposit: Lovelace,
    pub drep: Option<(DRep, CertificatePointer)>,
    pub rewards: Lovelace,
}

impl<C> cbor::encode::Encode<C> for Row {
    fn encode<W: cbor::encode::Write>(
        &self,
        e: &mut cbor::Encoder<W>,
        _ctx: &mut C,
    ) -> Result<(), cbor::encode::Error<W::Error>> {
        e.array(4)?;
        encode_optional_entry(e, self.pool.as_ref())?;
        e.u64(self.deposit)?;
        encode_optional_entry(e, self.drep.as_ref())?;
        e.u64(self.rewards)?;
        Ok(())
    }
}

impl<'a, C> cbor::decode::Decode<'a, C> for Row {
    fn decode(d: &mut cbor::Decoder<'a>, _ctx: &mut C) -> Result<Self, cbor::decode::Error> {
        d.array()?;
        Ok(Row {
            pool: decode_optional_entry(d)?,
            deposit: d.u64()?,
            drep: decode_optional_entry(d)?,
            rewards: d.u64()?,
        })
    }
}

#[derive(Clone)]
struct BenchmarkConfig {
    entries: usize,
    key_size: usize,
    value_size: usize,
    namespaces: usize,
    subset_size: usize,
    out_dir: PathBuf,
    criterion_dir: PathBuf,
    criterion_sample_size: usize,
    criterion_warm_up: Duration,
    criterion_measurement: Duration,
    criterion_resamples: usize,
    backends: Vec<String>,
}

impl BenchmarkConfig {
    fn to_report_config(&self) -> ReportConfig {
        ReportConfig {
            entries: self.entries,
            key_size: self.key_size,
            value_size: self.value_size,
            namespaces: self.namespaces,
            subset_size: self.subset_size,
        }
    }

    fn apply_child_env(&self, command: &mut Command) {
        command
            .env("CBOR_DB_BENCH_OUT_DIR", &self.out_dir)
            .env("CBOR_DB_BENCH_CRITERION_DIR", &self.criterion_dir)
            .env("CBOR_DB_BENCH_ENTRIES", self.entries.to_string())
            .env("CBOR_DB_BENCH_KEY_SIZE", self.key_size.to_string())
            .env("CBOR_DB_BENCH_VALUE_SIZE", self.value_size.to_string())
            .env("CBOR_DB_BENCH_SUBSET_SIZE", self.subset_size.to_string())
            .env("CBOR_DB_BENCH_NAMESPACES", self.namespaces.to_string())
            .env(
                "CBOR_DB_BENCH_SAMPLE_SIZE",
                self.criterion_sample_size.to_string(),
            )
            .env(
                "CBOR_DB_BENCH_WARM_UP_MS",
                self.criterion_warm_up.as_millis().to_string(),
            )
            .env(
                "CBOR_DB_BENCH_MEASUREMENT_MS",
                self.criterion_measurement.as_millis().to_string(),
            )
            .env(
                "CBOR_DB_BENCH_RESAMPLES",
                self.criterion_resamples.to_string(),
            );
    }
}

#[derive(Clone)]
struct Operation {
    key: Vec<u8>,
    row: Row,
    value: Vec<u8>,
    reward_value: Vec<u8>,
    full_updated_value: Vec<u8>,
    partial_updated_value: Vec<u8>,
}

struct BackendFactory {
    name: &'static str,
    create: fn(&Path) -> Result<Box<dyn StorageBackend>, String>,
}

#[derive(Deserialize)]
struct CriterionEstimates {
    mean: CriterionStatistic,
    slope: Option<CriterionStatistic>,
}

#[derive(Deserialize)]
struct CriterionStatistic {
    point_estimate: f64,
}

type DynStore = Store<Box<dyn StorageBackend>>;

const STATIC_NAMESPACE: &[u8] = b"row_static";
const REWARDS_PATH: [usize; 1] = [3];
const FIXED_WIDTH_ZERO_U64: [u8; 9] = [0x1B, 0, 0, 0, 0, 0, 0, 0, 0];
const CHILD_BACKEND_ENV: &str = "CBOR_DB_BENCH_ONLY_BACKEND";
const CHILD_RESULT_PATH_ENV: &str = "CBOR_DB_BENCH_CHILD_RESULT";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config()?;
    fs::create_dir_all(&config.out_dir)?;
    fs::create_dir_all(&config.criterion_dir)?;

    if let Ok(backend_name) = env::var(CHILD_BACKEND_ENV) {
        let result_path = PathBuf::from(env::var(CHILD_RESULT_PATH_ENV)?);
        run_child_backend(&config, &backend_name, &result_path)?;
        return Ok(());
    }

    run_parent_benchmarks(&config)
}

fn run_parent_benchmarks(config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
    let intermediate_dir = config.out_dir.join("backend-results");
    fs::create_dir_all(&intermediate_dir)?;

    let mut results = Vec::new();
    let executable = env::current_exe()?;

    for backend_name in &config.backends {
        let result_path = intermediate_dir.join(format!("{backend_name}.json"));
        if result_path.exists() {
            fs::remove_file(&result_path)?;
        }

        let mut command = Command::new(&executable);
        command
            .env(CHILD_BACKEND_ENV, backend_name)
            .env(CHILD_RESULT_PATH_ENV, &result_path);
        config.apply_child_env(&mut command);
        let status = command.status()?;

        if !status.success() {
            return Err(format!("benchmark child failed for backend '{backend_name}'").into());
        }

        let result = serde_json::from_slice::<BackendReport>(&fs::read(&result_path)?)?;
        results.push(result);
    }

    results.sort_by(|left, right| {
        let left_rank = backend_display_rank(&left.backend);
        let right_rank = backend_display_rank(&right.backend);

        left_rank.cmp(&right_rank).then_with(|| {
            right
                .partial_get_static
                .ops_per_sec
                .partial_cmp(&left.partial_get_static.ops_per_sec)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    });

    let report = BenchmarkReport {
        config: config.to_report_config(),
        results,
        generated_at_unix_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    };

    write_outputs(&config.out_dir, &report)?;
    Ok(())
}

fn backend_display_rank(name: &str) -> u8 {
    match name {
        "memory" => 0,
        "rocksdb" => 1,
        _ => 2,
    }
}

fn run_child_backend(
    config: &BenchmarkConfig,
    backend_name: &str,
    result_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let dataset = generate_operations(config)?;
    let factories = backend_factories();
    let factory = factories
        .iter()
        .find(|factory| factory.name == backend_name)
        .ok_or_else(|| format!("unknown backend '{backend_name}'"))?;

    let result = benchmark_backend(factory, config, &dataset)?;
    if let Some(parent) = result_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(result_path, serde_json::to_vec_pretty(&result)?)?;
    Ok(())
}

fn benchmark_backend(
    factory: &BackendFactory,
    config: &BenchmarkConfig,
    dataset: &[Operation],
) -> Result<BackendReport, Box<dyn std::error::Error>> {
    let backend_root = config.out_dir.join("data").join(factory.name);
    reset_directory(&backend_root)?;

    let memory = run_validation_profile(factory, &backend_root, dataset)?;
    let on_disk_bytes = measure_backend_disk_usage(factory.name, &backend_root.join("raw"))?;

    let mut criterion = build_criterion(config);
    run_criterion_benchmarks(&mut criterion, factory, &backend_root, dataset)?;
    criterion.final_summary();

    Ok(BackendReport {
        backend: factory.name.to_string(),
        on_disk_bytes,
        memory,
        insert: load_criterion_metric(config, factory.name, "insert", dataset.len())?,
        full_read: load_criterion_metric(config, factory.name, "full_read", dataset.len())?,
        partial_get_static: load_criterion_metric(
            config,
            factory.name,
            "partial_get_static",
            dataset.len(),
        )?,
        full_update_static: load_criterion_metric(
            config,
            factory.name,
            "full_update_static",
            dataset.len(),
        )?,
        partial_update_static: load_criterion_metric(
            config,
            factory.name,
            "partial_update_static",
            dataset.len(),
        )?,
        delete: load_criterion_metric(config, factory.name, "delete", dataset.len())?,
    })
}

fn run_validation_profile(
    factory: &BackendFactory,
    backend_root: &Path,
    dataset: &[Operation],
) -> Result<MemoryProfile, Box<dyn std::error::Error>> {
    let raw_backend_dir = backend_root.join("raw");
    fs::create_dir_all(&raw_backend_dir)?;

    let mut memory = ProcessMemoryTracker::new()?;
    let rss_before_bytes = memory.observe()?;

    let backend = (factory.create)(&raw_backend_dir)?;
    let store = Store::open(backend)?;
    memory.observe()?;

    let _ = bench_insert(&store, dataset)?;
    memory.observe()?;
    let _ = bench_full_read(&store, dataset)?;
    memory.observe()?;
    let _ = bench_partial_get_static(&store, dataset)?;
    memory.observe()?;
    let _ = bench_full_update_static(factory, &backend_root.join("full-update-static"), dataset)?;
    memory.observe()?;
    let _ = bench_partial_update_static(
        factory,
        &backend_root.join("partial-update-static"),
        dataset,
    )?;
    memory.observe()?;
    let _ = bench_delete(factory, &backend_root.join("delete"), dataset)?;
    memory.observe()?;

    drop(store);
    let rss_after_bytes = memory.observe()?;

    Ok(MemoryProfile {
        rss_before_bytes,
        rss_peak_bytes: memory.peak_bytes(),
        rss_after_bytes,
    })
}

fn run_criterion_benchmarks(
    criterion: &mut Criterion,
    factory: &BackendFactory,
    backend_root: &Path,
    dataset: &[Operation],
) -> Result<(), String> {
    let group_name = format!("backend_{}", factory.name);
    let mut group = criterion.benchmark_group(group_name);
    group.throughput(Throughput::Elements(dataset.len() as u64));

    let insert_root = backend_root.join("criterion").join("insert");
    let mut insert_iteration = 0_u64;
    group.bench_function("insert", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                let store =
                    prepare_empty_store(factory, &iteration_dir(&insert_root, insert_iteration))
                        .expect("insert setup must succeed");
                insert_iteration += 1;

                let started = Instant::now();
                seed_namespace(&store, STATIC_NAMESPACE, dataset)
                    .expect("insert benchmark must succeed");
                elapsed += started.elapsed();
            }
            elapsed
        });
    });

    let full_read_store = prepare_seeded_store(
        factory,
        &backend_root.join("criterion").join("full-read"),
        STATIC_NAMESPACE,
        dataset,
    )?;
    group.bench_function("full_read", |b| {
        b.iter(|| run_full_read_pass(&full_read_store, dataset).expect("full read must succeed"));
    });

    let partial_get_store = prepare_seeded_store(
        factory,
        &backend_root.join("criterion").join("partial-get-static"),
        STATIC_NAMESPACE,
        dataset,
    )?;
    group.bench_function("partial_get_static", |b| {
        b.iter(|| {
            run_partial_get_static_pass(&partial_get_store, dataset)
                .expect("partial get benchmark must succeed")
        });
    });

    let full_update_root = backend_root.join("criterion").join("full-update-static");
    let mut full_update_iteration = 0_u64;
    group.bench_function("full_update_static", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                let store = prepare_seeded_store(
                    factory,
                    &iteration_dir(&full_update_root, full_update_iteration),
                    STATIC_NAMESPACE,
                    dataset,
                )
                .expect("full update setup must succeed");
                full_update_iteration += 1;

                let started = Instant::now();
                run_full_update_static_pass(&store, dataset)
                    .expect("full update benchmark must succeed");
                elapsed += started.elapsed();
            }
            elapsed
        });
    });

    let partial_update_root = backend_root.join("criterion").join("partial-update-static");
    let mut partial_update_iteration = 0_u64;
    group.bench_function("partial_update_static", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                let store = prepare_seeded_store(
                    factory,
                    &iteration_dir(&partial_update_root, partial_update_iteration),
                    STATIC_NAMESPACE,
                    dataset,
                )
                .expect("partial update setup must succeed");
                partial_update_iteration += 1;

                let started = Instant::now();
                run_partial_update_static_pass(&store, dataset)
                    .expect("partial update benchmark must succeed");
                elapsed += started.elapsed();
            }
            elapsed
        });
    });

    let delete_root = backend_root.join("criterion").join("delete");
    let mut delete_iteration = 0_u64;
    group.bench_function("delete", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                let store = prepare_seeded_store(
                    factory,
                    &iteration_dir(&delete_root, delete_iteration),
                    STATIC_NAMESPACE,
                    dataset,
                )
                .expect("delete setup must succeed");
                delete_iteration += 1;

                let started = Instant::now();
                run_delete_pass(&store, dataset).expect("delete benchmark must succeed");
                elapsed += started.elapsed();
            }
            elapsed
        });
    });

    group.finish();
    Ok(())
}

fn build_criterion(config: &BenchmarkConfig) -> Criterion {
    Criterion::default()
        .sample_size(config.criterion_sample_size)
        .warm_up_time(config.criterion_warm_up)
        .measurement_time(config.criterion_measurement)
        .nresamples(config.criterion_resamples)
        .output_directory(config.criterion_dir.as_path())
        .without_plots()
}

fn load_criterion_metric(
    config: &BenchmarkConfig,
    backend_name: &str,
    benchmark_name: &str,
    operations: usize,
) -> Result<Metric, Box<dyn std::error::Error>> {
    let estimate_path = config
        .criterion_dir
        .join(format!("backend_{backend_name}"))
        .join(benchmark_name)
        .join("new")
        .join("estimates.json");

    let estimates = serde_json::from_slice::<CriterionEstimates>(&fs::read(&estimate_path)?)?;
    let point_estimate_ns = estimates
        .slope
        .as_ref()
        .unwrap_or(&estimates.mean)
        .point_estimate;
    Ok(metric_from_estimate(operations, point_estimate_ns))
}

fn metric_from_estimate(operations: usize, point_estimate_ns: f64) -> Metric {
    let seconds = point_estimate_ns.max(0.0) / 1_000_000_000.0;
    let elapsed = Duration::from_secs_f64(seconds);
    Metric {
        operations,
        elapsed_ms: elapsed.as_millis(),
        elapsed_us: elapsed.as_micros(),
        ops_per_sec: if seconds == 0.0 {
            operations as f64
        } else {
            operations as f64 / seconds
        },
    }
}

fn measure_backend_disk_usage(
    backend_name: &str,
    backend_dir: &Path,
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    if backend_name == "memory" {
        return Ok(None);
    }

    Ok(Some(directory_size_bytes(backend_dir)?))
}

fn directory_size_bytes(path: &Path) -> Result<u64, Box<dyn std::error::Error>> {
    let mut total_bytes = 0_u64;

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        let metadata = entry.metadata()?;

        if metadata.is_dir() {
            total_bytes = total_bytes.saturating_add(directory_size_bytes(&entry_path)?);
        } else if metadata.is_file() {
            total_bytes = total_bytes.saturating_add(metadata.len());
        }
    }

    Ok(total_bytes)
}

fn iteration_dir(root: &Path, iteration: u64) -> PathBuf {
    root.join(format!("run-{iteration}"))
}

fn load_config() -> Result<BenchmarkConfig, Box<dyn std::error::Error>> {
    let criterion_sample_size = read_env_usize("CBOR_DB_BENCH_SAMPLE_SIZE", 20)?;
    if criterion_sample_size < 10 {
        return Err("CBOR_DB_BENCH_SAMPLE_SIZE must be at least 10".into());
    }

    let criterion_resamples = read_env_usize("CBOR_DB_BENCH_RESAMPLES", 20_000)?;
    if criterion_resamples == 0 {
        return Err("CBOR_DB_BENCH_RESAMPLES must be greater than 0".into());
    }

    let criterion_warm_up_ms = read_env_u64("CBOR_DB_BENCH_WARM_UP_MS", 500)?;
    if criterion_warm_up_ms == 0 {
        return Err("CBOR_DB_BENCH_WARM_UP_MS must be greater than 0".into());
    }

    let criterion_measurement_ms = read_env_u64("CBOR_DB_BENCH_MEASUREMENT_MS", 2_000)?;
    if criterion_measurement_ms == 0 {
        return Err("CBOR_DB_BENCH_MEASUREMENT_MS must be greater than 0".into());
    }

    Ok(BenchmarkConfig {
        entries: read_env_usize("CBOR_DB_BENCH_ENTRIES", 4_000)?,
        key_size: read_env_usize("CBOR_DB_BENCH_KEY_SIZE", 24)?,
        value_size: read_env_usize("CBOR_DB_BENCH_VALUE_SIZE", 256)?,
        namespaces: read_env_usize("CBOR_DB_BENCH_NAMESPACES", 4)?,
        subset_size: read_env_usize("CBOR_DB_BENCH_SUBSET_SIZE", 16)?,
        out_dir: PathBuf::from(
            env::var("CBOR_DB_BENCH_OUT_DIR").unwrap_or_else(|_| "docs/benchmarks".to_string()),
        ),
        criterion_dir: PathBuf::from(
            env::var("CBOR_DB_BENCH_CRITERION_DIR")
                .unwrap_or_else(|_| "target/criterion".to_string()),
        ),
        criterion_sample_size,
        criterion_warm_up: Duration::from_millis(criterion_warm_up_ms),
        criterion_measurement: Duration::from_millis(criterion_measurement_ms),
        criterion_resamples,
        backends: env::var("CBOR_DB_BENCH_BACKENDS")
            .unwrap_or_else(|_| "memory,sled,rocksdb,fjall,surrealkv,tidehunter,turso".to_string())
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
    })
}

fn read_env_usize(key: &str, default: usize) -> Result<usize, Box<dyn std::error::Error>> {
    match env::var(key) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(default),
    }
}

fn read_env_u64(key: &str, default: u64) -> Result<u64, Box<dyn std::error::Error>> {
    match env::var(key) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(default),
    }
}

fn generate_operations(config: &BenchmarkConfig) -> Result<Vec<Operation>, String> {
    (0..config.entries)
        .map(|index| build_operation(index as u64, config))
        .collect()
}

fn build_operation(index: u64, config: &BenchmarkConfig) -> Result<Operation, String> {
    let row = build_row(index);
    let value = encode_row_bytes(&row)?;
    let reward_value = encode_u64(row.rewards)?;

    if reward_value.len() != FIXED_WIDTH_ZERO_U64.len() {
        return Err(format!(
            "row rewards must encode to {} bytes for partial update, got {}",
            FIXED_WIDTH_ZERO_U64.len(),
            reward_value.len()
        ));
    }

    let mut updated_row = row.clone();
    updated_row.rewards = 0;

    let full_updated_value = encode_row_bytes(&updated_row)?;
    let partial_updated_value = build_partial_updated_row_bytes(&value)?;

    Ok(Operation {
        key: build_key(index, config),
        row,
        value,
        reward_value,
        full_updated_value,
        partial_updated_value,
    })
}

fn build_row(index: u64) -> Row {
    Row {
        pool: Some((
            to_fixed_28(seed_bytes(index ^ 0xA5A5_5A5A, 28)),
            (
                wide_u64(index, 11),
                wide_u64(index, 12),
                wide_u64(index, 13),
            ),
        )),
        deposit: wide_u64(index, 21),
        drep: Some((
            to_fixed_28(seed_bytes(index ^ 0x5A5A_A5A5, 28)),
            (
                wide_u64(index, 31),
                wide_u64(index, 32),
                wide_u64(index, 33),
            ),
        )),
        rewards: wide_u64(index, 41),
    }
}

fn build_key(index: u64, config: &BenchmarkConfig) -> Vec<u8> {
    let mut key = seed_bytes(index ^ 0xD3AD_B33F, config.key_size.max(8));
    if config.key_size >= 4 {
        let bucket = (index as usize % config.namespaces.max(1)) as u32;
        key[..4].copy_from_slice(&bucket.to_be_bytes());
    }
    key.truncate(config.key_size);
    key
}

fn seed_namespace(store: &DynStore, namespace: &[u8], dataset: &[Operation]) -> Result<(), String> {
    for operation in dataset {
        store.insert(namespace, &operation.key, operation.value.clone())?;
    }
    Ok(())
}

fn bench_insert(store: &DynStore, dataset: &[Operation]) -> Result<Metric, String> {
    let started = Instant::now();
    seed_namespace(store, STATIC_NAMESPACE, dataset)?;
    Ok(metric(dataset.len(), started.elapsed()))
}

fn bench_full_read(store: &DynStore, dataset: &[Operation]) -> Result<Metric, String> {
    let started = Instant::now();
    run_full_read_pass(store, dataset)?;
    Ok(metric(dataset.len(), started.elapsed()))
}

fn run_full_read_pass(store: &DynStore, dataset: &[Operation]) -> Result<(), String> {
    for operation in dataset {
        let value =
            get!(store, b"row_static", operation.key.as_slice()).map_err(|err| err.to_string())?;
        let decoded = decode_row_bytes(value.as_ref())?;
        if decoded != operation.row {
            return Err("full read returned unexpected row".to_string());
        }
    }
    Ok(())
}

fn bench_partial_get_static(store: &DynStore, dataset: &[Operation]) -> Result<Metric, String> {
    let started = Instant::now();
    run_partial_get_static_pass(store, dataset)?;
    Ok(metric(dataset.len(), started.elapsed()))
}

fn run_partial_get_static_pass(store: &DynStore, dataset: &[Operation]) -> Result<(), String> {
    for operation in dataset {
        let value = get!(store, b"row_static", operation.key.as_slice(), rewards)
            .map_err(|err| err.to_string())?;
        if value.as_ref() != operation.reward_value.as_slice() {
            return Err("static partial get returned unexpected rewards bytes".to_string());
        }
    }
    Ok(())
}

fn bench_full_update_static(
    factory: &BackendFactory,
    backend_dir: &Path,
    dataset: &[Operation],
) -> Result<Metric, String> {
    bench_update_static(factory, backend_dir, dataset, false)
}

fn bench_partial_update_static(
    factory: &BackendFactory,
    backend_dir: &Path,
    dataset: &[Operation],
) -> Result<Metric, String> {
    bench_update_static(factory, backend_dir, dataset, true)
}

fn bench_delete(
    factory: &BackendFactory,
    backend_dir: &Path,
    dataset: &[Operation],
) -> Result<Metric, String> {
    let store = prepare_seeded_store(factory, backend_dir, STATIC_NAMESPACE, dataset)?;

    let started = Instant::now();
    run_delete_pass(&store, dataset)?;
    let result = metric(dataset.len(), started.elapsed());

    for operation in dataset {
        match store.get(STATIC_NAMESPACE, &operation.key) {
            Err(StoreError::NotFound) => {}
            Ok(_) => return Err("delete left a value behind".to_string()),
            Err(err) => return Err(err.to_string()),
        }
    }

    Ok(result)
}

fn run_delete_pass(store: &DynStore, dataset: &[Operation]) -> Result<(), String> {
    for operation in dataset {
        store.delete(STATIC_NAMESPACE, &operation.key)?;
    }
    Ok(())
}

fn prepare_empty_store(factory: &BackendFactory, backend_dir: &Path) -> Result<DynStore, String> {
    reset_directory(backend_dir)?;
    let backend = (factory.create)(backend_dir)?;
    Store::open(backend)
}

fn prepare_seeded_store(
    factory: &BackendFactory,
    backend_dir: &Path,
    namespace: &[u8],
    dataset: &[Operation],
) -> Result<DynStore, String> {
    let store = prepare_empty_store(factory, backend_dir)?;
    seed_namespace(&store, namespace, dataset)?;
    Ok(store)
}

fn bench_update_static(
    factory: &BackendFactory,
    backend_dir: &Path,
    dataset: &[Operation],
    partial: bool,
) -> Result<Metric, String> {
    let store = prepare_seeded_store(factory, backend_dir, STATIC_NAMESPACE, dataset)?;

    let started = Instant::now();
    if partial {
        run_partial_update_static_pass(&store, dataset)?;
    } else {
        run_full_update_static_pass(&store, dataset)?;
    }
    let result = metric(dataset.len(), started.elapsed());

    verify_full_values(&store, STATIC_NAMESPACE, dataset, partial)?;
    Ok(result)
}

fn run_full_update_static_pass(store: &DynStore, dataset: &[Operation]) -> Result<(), String> {
    for operation in dataset {
        update!(
            store,
            b"row_static",
            operation.key.as_slice(),
            |value: &mut Vec<u8>| rewrite_row_rewards_to_zero(value)
        )?;
    }
    Ok(())
}

fn run_partial_update_static_pass(store: &DynStore, dataset: &[Operation]) -> Result<(), String> {
    for operation in dataset {
        update!(
            store,
            b"row_static",
            operation.key.as_slice(),
            rewards,
            |value: &mut [u8]| overwrite_reward_with_fixed_width_zero(value)
        )?;
    }
    Ok(())
}

fn verify_full_values(
    store: &DynStore,
    namespace: &[u8],
    dataset: &[Operation],
    partial: bool,
) -> Result<(), String> {
    for operation in dataset {
        let value = store
            .get(namespace, &operation.key)
            .map_err(|err| err.to_string())?;
        let expected = if partial {
            &operation.partial_updated_value
        } else {
            &operation.full_updated_value
        };

        if value.as_ref() != expected.as_slice() {
            return Err("update returned unexpected payload".to_string());
        }
    }
    Ok(())
}

fn rewrite_row_rewards_to_zero(value: &mut Vec<u8>) {
    let mut row = decode_row_bytes(value).expect("stored row must decode");
    row.rewards = 0;
    *value = encode_row_bytes(&row).expect("stored row must re-encode");
}

fn overwrite_reward_with_fixed_width_zero(value: &mut [u8]) {
    value.copy_from_slice(&FIXED_WIDTH_ZERO_U64);
}

fn build_partial_updated_row_bytes(value: &[u8]) -> Result<Vec<u8>, String> {
    let mut updated = value.to_vec();
    let (start, len) = store::navigator::navigate_to_offset(&updated, &REWARDS_PATH)
        .map_err(|err| err.to_string())?;
    overwrite_reward_with_fixed_width_zero(&mut updated[start..start + len]);
    Ok(updated)
}

fn encode_row_bytes(row: &Row) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut encoder = cbor::Encoder::new(&mut out);
    row.encode(&mut encoder, &mut ())
        .map_err(|err| err.to_string())?;
    Ok(out)
}

fn decode_row_bytes(bytes: &[u8]) -> Result<Row, String> {
    let mut decoder = cbor::Decoder::new(bytes);
    Row::decode(&mut decoder, &mut ()).map_err(|err| err.to_string())
}

fn encode_optional_entry<W: cbor::encode::Write>(
    encoder: &mut cbor::Encoder<W>,
    entry: Option<&([u8; 28], CertificatePointer)>,
) -> Result<(), cbor::encode::Error<W::Error>> {
    match entry {
        Some((id, pointer)) => {
            encoder.array(2)?;
            encoder.bytes(id)?;
            encode_certificate_pointer(encoder, *pointer)?;
        }
        None => {
            encoder.null()?;
        }
    }
    Ok(())
}

fn encode_certificate_pointer<W: cbor::encode::Write>(
    encoder: &mut cbor::Encoder<W>,
    pointer: CertificatePointer,
) -> Result<(), cbor::encode::Error<W::Error>> {
    encoder.array(3)?;
    encoder.u64(pointer.0)?;
    encoder.u64(pointer.1)?;
    encoder.u64(pointer.2)?;
    Ok(())
}

fn decode_optional_entry(
    decoder: &mut cbor::Decoder<'_>,
) -> Result<Option<([u8; 28], CertificatePointer)>, cbor::decode::Error> {
    match decoder.datatype()? {
        Type::Null => {
            decoder.null()?;
            Ok(None)
        }
        _ => {
            decoder.array()?;
            let id = decode_fixed_28(decoder)?;
            let pointer = decode_certificate_pointer(decoder)?;
            Ok(Some((id, pointer)))
        }
    }
}

fn decode_certificate_pointer(
    decoder: &mut cbor::Decoder<'_>,
) -> Result<CertificatePointer, cbor::decode::Error> {
    decoder.array()?;
    Ok((decoder.u64()?, decoder.u64()?, decoder.u64()?))
}

fn decode_fixed_28(decoder: &mut cbor::Decoder<'_>) -> Result<[u8; 28], cbor::decode::Error> {
    let bytes = decoder.bytes()?;
    let slice: [u8; 28] = bytes
        .try_into()
        .map_err(|_| cbor::decode::Error::message("expected 28 bytes"))?;
    Ok(slice)
}

fn encode_u64(value: u64) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(9);
    let mut encoder = cbor::Encoder::new(&mut out);
    encoder.u64(value).map_err(|err| err.to_string())?;
    Ok(out)
}

fn to_fixed_28(bytes: Vec<u8>) -> [u8; 28] {
    bytes.try_into().expect("expected 28-byte buffer")
}

fn wide_u64(index: u64, salt: u64) -> u64 {
    (1_u64 << 40) + index.saturating_mul(97) + salt
}

fn seed_bytes(mut seed: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        out.extend_from_slice(&seed.to_le_bytes());
    }
    out.truncate(len);
    out
}

fn metric(operations: usize, elapsed: Duration) -> Metric {
    let seconds = elapsed.as_secs_f64();
    Metric {
        operations,
        elapsed_ms: elapsed.as_millis(),
        elapsed_us: elapsed.as_micros(),
        ops_per_sec: if seconds == 0.0 {
            operations as f64
        } else {
            operations as f64 / seconds
        },
    }
}

fn write_outputs(
    out_dir: &Path,
    report: &BenchmarkReport,
) -> Result<(), Box<dyn std::error::Error>> {
    let json = serde_json::to_string_pretty(report)?;
    fs::write(out_dir.join("results.json"), json)?;
    Ok(())
}

struct ProcessMemoryTracker {
    pid: sysinfo::Pid,
    peak_bytes: u64,
    system: System,
}

impl ProcessMemoryTracker {
    fn new() -> Result<Self, String> {
        let pid = sysinfo::get_current_pid().map_err(|err| err.to_string())?;
        let mut tracker = Self {
            pid,
            peak_bytes: 0,
            system: System::new(),
        };
        tracker.observe()?;
        Ok(tracker)
    }

    fn observe(&mut self) -> Result<u64, String> {
        self.system
            .refresh_processes(ProcessesToUpdate::Some(&[self.pid]), true);
        let process = self
            .system
            .process(self.pid)
            .ok_or_else(|| "failed to sample current process memory".to_string())?;
        let rss_bytes = process.memory();
        self.peak_bytes = self.peak_bytes.max(rss_bytes);
        Ok(rss_bytes)
    }

    fn peak_bytes(&self) -> u64 {
        self.peak_bytes
    }
}

fn reset_directory(path: &Path) -> Result<(), String> {
    if path.exists() {
        fs::remove_dir_all(path).map_err(|err| err.to_string())?;
    }
    fs::create_dir_all(path).map_err(|err| err.to_string())?;
    Ok(())
}

fn backend_factories() -> Vec<BackendFactory> {
    vec![
        BackendFactory {
            name: "memory",
            create: |_| Ok(Box::new(MemoryBackend::new())),
        },
        BackendFactory {
            name: "sled",
            create: |path| {
                Ok(Box::new(SledBackend::open(
                    path.to_str()
                        .ok_or_else(|| "invalid sled path".to_string())?,
                )?))
            },
        },
        BackendFactory {
            name: "rocksdb",
            create: |path| Ok(Box::new(RocksDbBackend::open(path)?)),
        },
        BackendFactory {
            name: "fjall",
            create: |path| Ok(Box::new(FjallBackend::open(path)?)),
        },
        BackendFactory {
            name: "surrealkv",
            create: |path| Ok(Box::new(SurrealKvBackend::open(path.to_path_buf())?)),
        },
        BackendFactory {
            name: "tidehunter",
            create: |path| Ok(Box::new(TidehunterBackend::open(path)?)),
        },
        BackendFactory {
            name: "turso",
            create: |path| Ok(Box::new(TursoBackend::open(path.join("store.db"))?)),
        },
    ]
}
