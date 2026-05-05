// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

//! Helpers for the `parquet-fast-bench` criterion harness.
//!
//! Three things live here:
//!
//! 1. [`LocalFileRangeReader`]: a [`delta_forge_io::range_reader::RangeReader`]
//!    that opens a local file fresh per call, seeks, reads. The "fresh
//!    per call" shape matches how the production engine drives many
//!    small ranged reads through a shared `Arc<dyn RangeReader>`; we
//!    keep an `Arc<std::sync::Mutex<File>>` per path so one
//!    `LocalFileRangeReader` services every call without forcing the
//!    open syscall into the per-read hot path.
//! 2. [`Fixtures`]: builds the three parquet files described in the
//!    bench plan (bulk-scan, selective-filter, point-lookup) into a
//!    [`tempfile::TempDir`]. Generated once at the start of the bench
//!    suite; reused by every benchmark group.
//! 3. Small reader-side helpers: a builder for a
//!    [`ParquetByteRange`] over a [`LocalFileRangeReader`], a
//!    metadata-fetch helper, and a fast-reader scan driver that
//!    consumes every row group end-to-end.
//!
//! Bench is local-FS only. Defeating the OS page cache is hard
//! cross-platform; criterion's per-iteration warm cache is honest
//! about that. To reason about cold-cache numbers, drop the buffer
//! cache between runs at the OS level (e.g. `sync && echo 3 >
//! /proc/sys/vm/drop_caches` on Linux, `Clear-RecycleBin` is NOT it,
//! Windows has no clean equivalent without third-party tools). The
//! harness reports relative numbers between the two readers on the
//! same warmth, which is the comparison we actually care about.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{
    ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::future::BoxFuture;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

use delta_forge_io::range_reader::RangeReader;
use delta_forge_parquet_fast::byte_range::ParquetByteRange;

/// Number of rows in the bulk-scan and selective-filter fixtures.
pub const ROWS_BULK: usize = 5_000_000;

/// Number of rows in the point-lookup fixture.
pub const ROWS_POINT: usize = 1_000_000;

/// One large row group for the bulk fixture: a single row group lets
/// the bulk path measure raw decode throughput without inter-rg
/// boundary dispatch overhead.
pub const ROW_GROUP_BULK: usize = ROWS_BULK;

/// Multi row group for the selective fixture: splitting into ~10
/// row groups exposes per-rg dispatch and lets predicate pushdown
/// (or post-decode filter) span more than one rg.
pub const ROW_GROUP_SELECTIVE: usize = 500_000;

/// Small row groups for the point-lookup fixture: ~50K rows each so
/// a `RowSelection` of a single row resolves to one small rg, and
/// the page index actually has multiple pages to skip across.
pub const ROW_GROUP_POINT: usize = 50_000;

/// Distinct value count for the dict-encoded string column in the
/// selective fixture. 100 distinct values means a small dictionary
/// page and a value-frequency of 1% per filter literal, which is the
/// "moderately selective" case where pushdown should win biggest.
pub const SELECTIVE_DICT_CARDINALITY: usize = 100;

/// `RangeReader` implementation backed by a local `std::fs::File`.
///
/// One file handle is opened at construction and reused across calls
/// behind a `Mutex`, matching the production assumption that a
/// `RangeReader` is `Arc`-shared and called concurrently from many
/// columns at once. The per-call cost is just `seek + read`, which is
/// what we want to measure: this isolates the parquet decoder cost
/// without folding in repeated `File::open` syscalls.
///
/// File handles are kept per path in a small map so a
/// `LocalFileRangeReader` can service multiple files. In the bench
/// each `ParquetByteRange` only ever reads one file, so the map is
/// effectively one entry; the structure mirrors a more general reader
/// shape so the bench code reads naturally.
#[derive(Debug)]
pub struct LocalFileRangeReader {
    files: Mutex<HashMap<String, File>>,
}

impl LocalFileRangeReader {
    /// Construct a reader that pre-opens a single file.
    pub fn for_path(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let p = path.as_ref();
        let key = path_key(p);
        let file = File::open(p)?;
        let mut map = HashMap::new();
        map.insert(key, file);
        Ok(Self {
            files: Mutex::new(map),
        })
    }

    /// Wrap as `Arc<dyn RangeReader>` for handing to
    /// [`ParquetByteRange::new`].
    pub fn into_arc(self) -> Arc<dyn RangeReader> {
        Arc::new(self)
    }

    fn read_locked(&self, path: &str, range: Range<u64>) -> std::io::Result<Bytes> {
        let mut guard = self.files.lock().expect("LocalFileRangeReader mutex poisoned");
        let file = match guard.get_mut(path) {
            Some(f) => f,
            None => {
                // Path wasn't pre-registered; open lazily so the
                // reader stays usable when the caller passes a path
                // string that differs from the canonical form used at
                // construction (Windows backslashes, prefix variations,
                // etc.). The first lazy open does pay one syscall; all
                // subsequent reads on the same path reuse the handle.
                let file = File::open(path)?;
                guard.insert(path.to_string(), file);
                guard
                    .get_mut(path)
                    .expect("just inserted handle missing from map")
            }
        };
        let len = (range.end - range.start) as usize;
        let mut buf = vec![0u8; len];
        file.seek(SeekFrom::Start(range.start))?;
        file.read_exact(&mut buf)?;
        Ok(Bytes::from(buf))
    }
}

impl RangeReader for LocalFileRangeReader {
    fn read_range<'a>(
        &'a self,
        path: &'a str,
        range: Range<u64>,
    ) -> BoxFuture<'a, std::io::Result<Bytes>> {
        // Synchronous IO inside an async future: the parquet-fast
        // decoder issues many concurrent ranged reads via
        // `try_join_all`, but each one resolves immediately on
        // local FS. No yield point is necessary; wrapping in
        // `async move` is enough to satisfy the trait contract.
        let path_owned = path.to_string();
        Box::pin(async move { self.read_locked(&path_owned, range) })
    }
}

#[inline]
fn path_key(p: &Path) -> String {
    // Use the platform string verbatim. The bench constructs reader
    // and `ParquetByteRange` from the same string, so equality holds.
    p.to_string_lossy().into_owned()
}

/// Three parquet fixtures held in a temp directory. Drop the
/// `Fixtures` value to delete the directory.
pub struct Fixtures {
    pub dir: TempDir,
    pub bulk: PathBuf,
    pub selective: PathBuf,
    pub point: PathBuf,
}

impl Fixtures {
    /// Build all three fixtures. Synchronous; called once from the
    /// bench's `criterion_group` setup. Total IO is moderate (well
    /// under 1 GiB); generation runs in a few seconds on a modern
    /// SSD.
    pub fn build() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("dfpf-bench-")
            .tempdir()
            .expect("create tempdir for parquet fixtures");
        let bulk = dir.path().join("bulk.parquet");
        let selective = dir.path().join("selective.parquet");
        let point = dir.path().join("point.parquet");
        write_bulk_fixture(&bulk);
        write_selective_fixture(&selective);
        write_point_fixture(&point);
        Self {
            dir,
            bulk,
            selective,
            point,
        }
    }
}

fn writer_props(row_group_size: usize) -> WriterProperties {
    // Knobs:
    // * SNAPPY: matches `delta-forge-parquet-io`'s flipped default.
    // * Page size 1 MiB: matches DataFusion / parquet-rs typical.
    // * Page index enabled: lets the fast reader's
    //   `offset_index_locator` spans-for-full-chunk path fire.
    // * Dictionary on by default: encodes the low-card string column
    //   in the selective fixture without any extra knob.
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(row_group_size)
        .set_data_page_row_count_limit(64 * 1024)
        .set_write_batch_size(8192)
        .set_dictionary_enabled(true)
        .set_encoding(Encoding::PLAIN)
        .build()
}

fn schema_bulk() -> Arc<Schema> {
    // 20 columns mixed types: 8 int64, 6 string-low-card, 4 double, 2
    // timestamp. Layout chosen so projection of "first 2 columns"
    // exercises both an int64 and a low-card string (the two cheap
    // physical types) and the wide-scan covers every type group.
    let mut fields = Vec::with_capacity(20);
    for i in 0..8 {
        fields.push(Field::new(format!("i_{i}"), DataType::Int64, false));
    }
    for i in 0..6 {
        fields.push(Field::new(format!("s_{i}"), DataType::Utf8, false));
    }
    for i in 0..4 {
        fields.push(Field::new(format!("d_{i}"), DataType::Float64, false));
    }
    for i in 0..2 {
        fields.push(Field::new(
            format!("t_{i}"),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ));
    }
    Arc::new(Schema::new(fields))
}

fn schema_selective() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("flag", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn schema_point() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]))
}

fn write_bulk_fixture(path: &Path) {
    let schema = schema_bulk();
    let file = File::create(path).expect("create bulk fixture");
    let mut writer = ArrowWriter::try_new(
        file,
        schema.clone(),
        Some(writer_props(ROW_GROUP_BULK)),
    )
    .expect("ArrowWriter::try_new bulk");
    // Write in 100K-row chunks so the writer can flush dict pages
    // periodically without exceeding RAM. The row-group size is
    // 5M, so all chunks land in one row group.
    let chunk_rows = 100_000usize;
    let mut produced = 0usize;
    while produced < ROWS_BULK {
        let take = std::cmp::min(chunk_rows, ROWS_BULK - produced);
        let arrays = build_bulk_arrays(produced, take);
        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .expect("bulk batch try_new");
        writer.write(&batch).expect("write bulk batch");
        produced += take;
    }
    writer.close().expect("close bulk writer");
}

fn build_bulk_arrays(start: usize, n: usize) -> Vec<ArrayRef> {
    let mut out: Vec<ArrayRef> = Vec::with_capacity(20);
    for c in 0..8 {
        let v: Vec<i64> = (0..n).map(|i| ((start + i) as i64).wrapping_mul((c + 1) as i64)).collect();
        out.push(Arc::new(Int64Array::from(v)));
    }
    for c in 0..6 {
        let v: Vec<String> = (0..n)
            .map(|i| {
                // 64 distinct values per column: low-card so dict
                // encoding kicks in. Different column index, different
                // namespace, so each column has its own dict.
                let bucket = (start + i) % 64;
                format!("c{c}_v{bucket:02}")
            })
            .collect();
        out.push(Arc::new(StringArray::from(v)));
    }
    for c in 0..4 {
        let v: Vec<f64> = (0..n)
            .map(|i| ((start + i) as f64) * 0.5_f64 + (c as f64))
            .collect();
        out.push(Arc::new(Float64Array::from(v)));
    }
    for _c in 0..2 {
        let v: Vec<i64> = (0..n)
            .map(|i| 1_700_000_000_000_000_i64 + ((start + i) as i64) * 1_000_000)
            .collect();
        out.push(Arc::new(TimestampMicrosecondArray::from(v)));
    }
    out
}

fn write_selective_fixture(path: &Path) {
    let schema = schema_selective();
    let file = File::create(path).expect("create selective fixture");
    let mut writer = ArrowWriter::try_new(
        file,
        schema.clone(),
        Some(writer_props(ROW_GROUP_SELECTIVE)),
    )
    .expect("ArrowWriter::try_new selective");
    let chunk_rows = 100_000usize;
    let mut produced = 0usize;
    while produced < ROWS_BULK {
        let take = std::cmp::min(chunk_rows, ROWS_BULK - produced);
        let id: Vec<i64> = ((produced as i64)..((produced + take) as i64)).collect();
        let category: Vec<String> = (0..take)
            .map(|i| {
                let bucket = (produced + i) % SELECTIVE_DICT_CARDINALITY;
                format!("cat_{bucket:03}")
            })
            .collect();
        let value: Vec<f64> = (0..take)
            .map(|i| ((produced + i) as f64) * 0.25_f64)
            .collect();
        let flag: Vec<i64> = (0..take)
            .map(|i| ((produced + i) % 7) as i64)
            .collect();
        let ts: Vec<i64> = (0..take)
            .map(|i| 1_700_000_000_000_000_i64 + ((produced + i) as i64) * 1_000_000)
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(id)),
                Arc::new(StringArray::from(category)),
                Arc::new(Float64Array::from(value)),
                Arc::new(Int64Array::from(flag)),
                Arc::new(TimestampMicrosecondArray::from(ts)),
            ],
        )
        .expect("selective batch try_new");
        writer.write(&batch).expect("write selective batch");
        produced += take;
    }
    writer.close().expect("close selective writer");
}

fn write_point_fixture(path: &Path) {
    let schema = schema_point();
    let file = File::create(path).expect("create point fixture");
    let mut writer = ArrowWriter::try_new(
        file,
        schema.clone(),
        Some(writer_props(ROW_GROUP_POINT)),
    )
    .expect("ArrowWriter::try_new point");
    let chunk_rows = 50_000usize;
    let mut produced = 0usize;
    while produced < ROWS_POINT {
        let take = std::cmp::min(chunk_rows, ROWS_POINT - produced);
        // Sorted INT64 key: monotonic, no gaps. Lets the page-index
        // path resolve a single-row selection to one page in one rg.
        let key: Vec<i64> = ((produced as i64)..((produced + take) as i64)).collect();
        let payload: Vec<String> = (0..take)
            .map(|i| format!("payload_{:08}", produced + i))
            .collect();
        let score: Vec<f64> = (0..take)
            .map(|i| ((produced + i) as f64).sqrt())
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(key)),
                Arc::new(StringArray::from(payload)),
                Arc::new(Float64Array::from(score)),
            ],
        )
        .expect("point batch try_new");
        writer.write(&batch).expect("write point batch");
        produced += take;
    }
    writer.close().expect("close point writer");
}

/// Open a `ParquetByteRange` over the file at `path`. Each call opens
/// a fresh file handle, so successive bench iterations do not share
/// the per-file `Seek` cursor between invocations.
pub fn open_byte_range(path: &Path) -> ParquetByteRange {
    let file_size = std::fs::metadata(path)
        .expect("metadata for byte-range open")
        .len();
    let reader = LocalFileRangeReader::for_path(path)
        .expect("LocalFileRangeReader::for_path");
    let path_str = path.to_string_lossy().into_owned();
    ParquetByteRange::new(reader.into_arc(), path_str, file_size)
}

/// Load and return parquet metadata via the parquet-rs sync reader
/// path. The fast reader consumes the resulting `ParquetMetaData`
/// directly; the parquet-rs reader rebuilds its own equivalent
/// internally. Reading the metadata once and handing it to both sides
/// is intentional: it isolates the row-group / page-decode cost from
/// the footer-parse cost, which is one round-trip either way.
pub fn load_metadata(path: &Path) -> Arc<ParquetMetaData> {
    use parquet::arrow::arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions,
    };
    use parquet::file::metadata::PageIndexPolicy;
    let file = File::open(path).expect("open for metadata");
    // Match `open_parquet_stream(page_index = true)`: load offset
    // index so the fast reader's locator sees per-page row offsets.
    let opts = ArrowReaderOptions::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_offset_index_policy(PageIndexPolicy::Required);
    let arrow_meta = ArrowReaderMetadata::load(&file, opts)
        .expect("ArrowReaderMetadata::load");
    arrow_meta.metadata().clone()
}
