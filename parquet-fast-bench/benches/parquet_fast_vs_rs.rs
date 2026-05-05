// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

//! `delta-forge-parquet-fast` vs upstream `parquet-rs`
//! microbenchmark.
//!
//! Four benchmark groups, each run twice (once per reader):
//!
//! | group               | fixture          | shape                                      |
//! | ------------------- | ---------------- | ------------------------------------------ |
//! | `bulk_scan`         | bulk             | full table scan, no filter, all 20 cols    |
//! | `selective_filter`  | selective        | RowFilter on `category = 'cat_007'`        |
//! | `point_lookup`      | point            | RowSelection of one row                    |
//! | `narrow_projection` | bulk             | project 2 of 20 cols, no filter            |
//!
//! Each iteration:
//!
//! - Opens the file fresh (constructs a new `ParquetByteRange` /
//!   `std::fs::File` handle). Local-FS page cache is warm; we cannot
//!   defeat it cross-platform from inside criterion. Drop OS cache
//!   between runs (`drop_caches` on Linux) to reason about cold
//!   numbers.
//! - Drives the chosen reader to consume every batch.
//! - Sums `num_rows` to discourage the optimizer from eliding the
//!   decode and to let criterion report `Throughput::Elements`.
//!
//! Run:
//!
//! ```text
//! cargo bench -p delta-forge-parquet-fast-bench
//! ```
//!
//! Run a single group:
//!
//! ```text
//! cargo bench -p delta-forge-parquet-fast-bench -- bulk_scan
//! ```

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, StringArray};
use arrow::record_batch::RecordBatch;
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
    RowFilter, RowSelection, RowSelector,
};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use tokio::runtime::Runtime;

use delta_forge_parquet_fast::byte_range::ParquetByteRange;
use delta_forge_parquet_fast::predicate_filter::apply_predicates;
use delta_forge_parquet_fast::row_group_decoder::row_group_to_batch;
use delta_forge_parquet_fast::row_selection_adapter::split_row_selection_per_row_group;

use delta_forge_parquet_fast_bench::{
    load_metadata, open_byte_range, Fixtures, ROWS_BULK, ROWS_POINT,
};

/// One-time process state: the temp dir holding the three fixtures
/// plus pre-loaded metadata so neither reader pays a footer round
/// trip per iteration. Wrapped in a static-y `OnceLock` so all four
/// bench groups share the same files without re-generating.
struct Suite {
    fixtures: Fixtures,
    bulk_meta: Arc<ParquetMetaData>,
    selective_meta: Arc<ParquetMetaData>,
    point_meta: Arc<ParquetMetaData>,
    rt: Runtime,
}

impl Suite {
    fn build() -> Self {
        let fixtures = Fixtures::build();
        let bulk_meta = load_metadata(&fixtures.bulk);
        let selective_meta = load_metadata(&fixtures.selective);
        let point_meta = load_metadata(&fixtures.point);
        // Single-thread runtime: the fast reader fans out within one
        // row group via `tokio::spawn`, but for fairness against the
        // sync parquet-rs path we run on one core. Multi-threading
        // skews the comparison toward whichever side parallelises
        // best, which is a follow-up bench, not this one.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio current_thread runtime");
        Self {
            fixtures,
            bulk_meta,
            selective_meta,
            point_meta,
            rt,
        }
    }
}

// ---------- parquet-rs side ---------------------------------------

/// Drive parquet-rs's sync `ParquetRecordBatchReader` to completion.
/// Returns the total row count consumed; criterion uses this only to
/// keep the optimizer honest.
fn parquet_rs_full_scan(path: &Path) -> usize {
    let file = std::fs::File::open(path).expect("open parquet-rs file");
    let opts = ArrowReaderOptions::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .with_offset_index_policy(PageIndexPolicy::Skip);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, opts)
        .expect("parquet-rs builder")
        .with_batch_size(8192);
    let reader = builder.build().expect("parquet-rs reader build");
    let mut rows = 0usize;
    for batch in reader {
        rows += batch.expect("parquet-rs batch").num_rows();
    }
    rows
}

fn parquet_rs_projection_scan(path: &Path, projection_roots: &[usize]) -> usize {
    let file = std::fs::File::open(path).expect("open parquet-rs file");
    let opts = ArrowReaderOptions::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .with_offset_index_policy(PageIndexPolicy::Skip);
    let mut builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, opts)
        .expect("parquet-rs builder")
        .with_batch_size(8192);
    let mask = ProjectionMask::roots(
        builder.parquet_schema(),
        projection_roots.iter().copied(),
    );
    builder = builder.with_projection(mask);
    let reader = builder.build().expect("parquet-rs reader build");
    let mut rows = 0usize;
    for batch in reader {
        rows += batch.expect("parquet-rs batch").num_rows();
    }
    rows
}

fn parquet_rs_row_filter_scan(path: &Path, filter_literal: &str) -> usize {
    let file = std::fs::File::open(path).expect("open parquet-rs file");
    let opts = ArrowReaderOptions::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .with_offset_index_policy(PageIndexPolicy::Skip);
    let mut builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, opts)
        .expect("parquet-rs builder")
        .with_batch_size(8192);
    // Filter column index 1 ("category") in the selective fixture.
    let filter_mask = ProjectionMask::roots(builder.parquet_schema(), [1usize]);
    let literal = filter_literal.to_string();
    let predicate = ArrowPredicateFn::new(filter_mask, move |batch: RecordBatch| {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("category column is StringArray");
        let mut out = Vec::with_capacity(col.len());
        for i in 0..col.len() {
            out.push(if col.is_null(i) {
                false
            } else {
                col.value(i) == literal
            });
        }
        Ok(BooleanArray::from(out))
    });
    builder = builder.with_row_filter(RowFilter::new(vec![Box::new(predicate)]));
    let reader = builder.build().expect("parquet-rs reader build");
    let mut rows = 0usize;
    for batch in reader {
        rows += batch.expect("parquet-rs batch").num_rows();
    }
    rows
}

fn parquet_rs_row_selection_scan(
    path: &Path,
    metadata: &ParquetMetaData,
    target_row: u64,
) -> usize {
    let file = std::fs::File::open(path).expect("open parquet-rs file");
    // Page-index loaded so the row selection drives page-skip inside
    // the row group it lands in. Mirrors the production
    // `with_page_index(true)` codepath in `open_parquet_stream`.
    let opts = ArrowReaderOptions::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_offset_index_policy(PageIndexPolicy::Required);
    let mut builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, opts)
        .expect("parquet-rs builder")
        .with_batch_size(8192);
    // RowSelection is over the entire scanned scope (all row groups),
    // so we build one continuous selector run: skip up to target_row,
    // select 1, skip the rest.
    let total: u64 = metadata.row_groups().iter().map(|rg| rg.num_rows() as u64).sum();
    let mut selectors: Vec<RowSelector> = Vec::with_capacity(3);
    if target_row > 0 {
        selectors.push(RowSelector::skip(target_row as usize));
    }
    selectors.push(RowSelector::select(1));
    let trailing = total - target_row - 1;
    if trailing > 0 {
        selectors.push(RowSelector::skip(trailing as usize));
    }
    builder = builder.with_row_selection(RowSelection::from(selectors));
    let reader = builder.build().expect("parquet-rs reader build");
    let mut rows = 0usize;
    for batch in reader {
        rows += batch.expect("parquet-rs batch").num_rows();
    }
    rows
}

// ---------- fast-reader side -------------------------------------

/// Run the fast reader's `row_group_to_batch` over every row group
/// in `metadata`, projecting the supplied leaf columns and applying
/// the supplied per-rg row mask (if any). Returns the total number
/// of rows decoded across all row groups.
async fn fast_full_scan(
    range: &ParquetByteRange,
    metadata: &ParquetMetaData,
    projected_leaves: &[usize],
) -> usize {
    let mut rows = 0usize;
    for rg_idx in 0..metadata.num_row_groups() {
        let batch = row_group_to_batch(range, metadata, rg_idx, projected_leaves, None)
            .await
            .expect("fast reader row_group_to_batch");
        rows += batch.num_rows();
    }
    rows
}

async fn fast_row_filter_scan(
    range: &ParquetByteRange,
    metadata: &ParquetMetaData,
    filter_literal: &str,
) -> usize {
    // The fast reader's RowFilter handling is post-decode, so we
    // mirror what `open_parquet_stream` does: project all leaves,
    // decode, then `apply_predicates` over the resulting batch.
    let schema_descr = metadata.file_metadata().schema_descr_ptr();
    let projected: Vec<usize> = (0..schema_descr.num_columns()).collect();
    let filter_mask = ProjectionMask::roots(&schema_descr, [1usize]);
    let literal = filter_literal.to_string();
    let predicate = ArrowPredicateFn::new(filter_mask, move |batch: RecordBatch| {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("category column is StringArray");
        let mut out = Vec::with_capacity(col.len());
        for i in 0..col.len() {
            out.push(if col.is_null(i) {
                false
            } else {
                col.value(i) == literal
            });
        }
        Ok(BooleanArray::from(out))
    });
    let mut predicates: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> =
        vec![Box::new(predicate)];
    let mut rows = 0usize;
    for rg_idx in 0..metadata.num_row_groups() {
        let batch = row_group_to_batch(range, metadata, rg_idx, &projected, None)
            .await
            .expect("fast reader row_group_to_batch");
        let filtered = apply_predicates(batch, &mut predicates, &projected)
            .expect("fast reader apply_predicates");
        rows += filtered.num_rows();
    }
    rows
}

async fn fast_row_selection_scan(
    range: &ParquetByteRange,
    metadata: &ParquetMetaData,
    target_row: u64,
) -> usize {
    let total: u64 = metadata.row_groups().iter().map(|rg| rg.num_rows() as u64).sum();
    let mut selectors: Vec<RowSelector> = Vec::with_capacity(3);
    if target_row > 0 {
        selectors.push(RowSelector::skip(target_row as usize));
    }
    selectors.push(RowSelector::select(1));
    let trailing = total - target_row - 1;
    if trailing > 0 {
        selectors.push(RowSelector::skip(trailing as usize));
    }
    let selection = RowSelection::from(selectors);
    let row_groups: Vec<usize> = (0..metadata.num_row_groups()).collect();
    let masks = split_row_selection_per_row_group(metadata, &row_groups, &selection)
        .expect("fast reader split_row_selection_per_row_group");
    let projected: Vec<usize> = (0..metadata.file_metadata().schema_descr().num_columns()).collect();
    let mut rows = 0usize;
    for (i, &rg_idx) in row_groups.iter().enumerate() {
        let mask = masks.get(i).cloned().flatten();
        if let Some(m) = mask.as_ref() {
            if m.is_empty() {
                continue;
            }
        }
        let batch = row_group_to_batch(
            range,
            metadata,
            rg_idx,
            &projected,
            mask.as_deref(),
        )
        .await
        .expect("fast reader row_group_to_batch with mask");
        rows += batch.num_rows();
    }
    rows
}

// ---------- benchmark groups -------------------------------------

fn bench_bulk_scan(c: &mut Criterion, suite: &Suite) {
    let mut group = c.benchmark_group("bulk_scan");
    group.throughput(Throughput::Elements(ROWS_BULK as u64));

    group.bench_function(BenchmarkId::new("parquet_rs", "bulk"), |b| {
        b.iter(|| {
            let n = parquet_rs_full_scan(&suite.fixtures.bulk);
            assert_eq!(n, ROWS_BULK);
        });
    });

    let n_leaves = suite.bulk_meta.file_metadata().schema_descr().num_columns();
    let projected_leaves: Vec<usize> = (0..n_leaves).collect();
    group.bench_function(BenchmarkId::new("fast_reader", "bulk"), |b| {
        b.iter(|| {
            // Re-open per iteration so each iteration sees the same
            // freshly-opened file handle the parquet-rs path does.
            let range = open_byte_range(&suite.fixtures.bulk);
            let n = suite.rt.block_on(fast_full_scan(
                &range,
                &suite.bulk_meta,
                &projected_leaves,
            ));
            assert_eq!(n, ROWS_BULK);
        });
    });

    group.finish();
}

fn bench_selective_filter(c: &mut Criterion, suite: &Suite) {
    let mut group = c.benchmark_group("selective_filter");
    group.throughput(Throughput::Elements(ROWS_BULK as u64));

    let literal = "cat_007";

    group.bench_function(BenchmarkId::new("parquet_rs", "filter_eq"), |b| {
        b.iter(|| {
            // ~1% selectivity: 1 of 100 dict values matches.
            let n = parquet_rs_row_filter_scan(&suite.fixtures.selective, literal);
            assert!(n > 0);
        });
    });

    group.bench_function(BenchmarkId::new("fast_reader", "filter_eq"), |b| {
        b.iter(|| {
            let range = open_byte_range(&suite.fixtures.selective);
            let n = suite.rt.block_on(fast_row_filter_scan(
                &range,
                &suite.selective_meta,
                literal,
            ));
            assert!(n > 0);
        });
    });

    group.finish();
}

fn bench_point_lookup(c: &mut Criterion, suite: &Suite) {
    let mut group = c.benchmark_group("point_lookup");
    // Throughput is "rows produced", which is 1 by definition. We
    // still want criterion to report it so the units are consistent
    // with the other groups.
    group.throughput(Throughput::Elements(1));

    // Mid-file row in a mid-row-group page; exercises both the page-
    // index lookup and a non-trivial within-rg offset.
    let target_row: u64 = (ROWS_POINT / 2) as u64 + 17;

    group.bench_function(BenchmarkId::new("parquet_rs", "single_row"), |b| {
        b.iter(|| {
            let n = parquet_rs_row_selection_scan(
                &suite.fixtures.point,
                &suite.point_meta,
                target_row,
            );
            assert_eq!(n, 1);
        });
    });

    group.bench_function(BenchmarkId::new("fast_reader", "single_row"), |b| {
        b.iter(|| {
            let range = open_byte_range(&suite.fixtures.point);
            let n = suite.rt.block_on(fast_row_selection_scan(
                &range,
                &suite.point_meta,
                target_row,
            ));
            assert_eq!(n, 1);
        });
    });

    group.finish();
}

fn bench_narrow_projection(c: &mut Criterion, suite: &Suite) {
    let mut group = c.benchmark_group("narrow_projection");
    group.throughput(Throughput::Elements(ROWS_BULK as u64));

    // Project root cols 0 (i_0, int64) and 8 (s_0, string-low-card).
    let projection_roots = [0usize, 8usize];

    group.bench_function(BenchmarkId::new("parquet_rs", "two_of_twenty"), |b| {
        b.iter(|| {
            let n = parquet_rs_projection_scan(
                &suite.fixtures.bulk,
                &projection_roots,
            );
            assert_eq!(n, ROWS_BULK);
        });
    });

    // Resolve the same root projection to leaf indices for the fast
    // reader. The bulk schema has no nested types so root == leaf,
    // but we route through ProjectionMask::roots to mirror what
    // `open_parquet_stream` does for nested-safe semantics.
    let schema_descr = suite.bulk_meta.file_metadata().schema_descr_ptr();
    let mask = ProjectionMask::roots(&schema_descr, projection_roots.iter().copied());
    let n_leaves = schema_descr.num_columns();
    let projected_leaves: Vec<usize> =
        (0..n_leaves).filter(|i| mask.leaf_included(*i)).collect();

    group.bench_function(BenchmarkId::new("fast_reader", "two_of_twenty"), |b| {
        b.iter(|| {
            let range = open_byte_range(&suite.fixtures.bulk);
            let n = suite.rt.block_on(fast_full_scan(
                &range,
                &suite.bulk_meta,
                &projected_leaves,
            ));
            assert_eq!(n, ROWS_BULK);
        });
    });

    group.finish();
}

// ---------- criterion plumbing -----------------------------------

/// One-shot fixture build, then four bench groups in sequence. The
/// `Suite` is held in a `OnceLock` so each `criterion_group!` entry
/// can borrow it without rebuilding fixtures.
fn all_benches(c: &mut Criterion) {
    use std::sync::OnceLock;
    static SUITE: OnceLock<Suite> = OnceLock::new();
    let suite = SUITE.get_or_init(Suite::build);
    bench_bulk_scan(c, suite);
    bench_selective_filter(c, suite);
    bench_point_lookup(c, suite);
    bench_narrow_projection(c, suite);
}

criterion_group!(benches, all_benches);
criterion_main!(benches);
