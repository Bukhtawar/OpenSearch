/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;

use datafusion::execution::cache::cache_manager::{CacheManagerConfig, CachedFileList};
use datafusion::execution::cache::{CacheAccessor, DefaultListFilesCache};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{col, lit};
use datafusion::{
    common::DataFusionError, datasource::listing::ListingTableUrl,
    execution::runtime_env::RuntimeEnvBuilder, physical_plan::displayable,
    physical_plan::execute_stream,
};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use log::error;
use native_bridge_common::log_debug;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use prost::Message;
use substrait::proto::Plan;

use crate::api::{DataFusionRuntime, ShardFileInfo};
use crate::cross_rt_stream::CrossRtStream;
use crate::executor::DedicatedExecutor;
use crate::helper::{
    build_query_runtime_env_with_store, build_query_session_context, register_listing_table,
};
use crate::session_context::SessionContextHandle;

/// BENCHMARK WIRING SWITCH for get-by-`__row_id__` (see `execute_query`).
/// `true`  → improved fast path (`point_read_by_row_id`); `false` → baseline full-query path.
/// Flip + recompile to A/B the two get-by-id paths on the same index.
const POINT_READ_MODE: bool = true;

/// Direct get-by-`__row_id__` point read. Bypasses the query engine entirely — no
/// SessionContext, no ListingTable registration, no logical/physical planning — which is
/// where the by-row-id path spent most of its CPU (analyzer + optimizer + create_physical_plan).
///
/// Locates the target physical row from the parquet footer (row-group prefix sum), reads
/// exactly that row via a `RowSelection` (all columns), and wraps the parquet stream exactly
/// like [`execute_query`] so the handle/return contract to Java is unchanged.
///
/// `row_id` is the physical `__row_id__` within this single-file shard view (the resolver
/// resolves an `_id` to a `(generation, rowId)`; the get path opens one file per generation).
async fn point_read_by_row_id(
    object_metas: Arc<Vec<ObjectMeta>>,
    store: Arc<dyn ObjectStore>,
    cpu_executor: DedicatedExecutor,
    runtime: &DataFusionRuntime,
    context_id: i64,
    row_id: i64,
) -> Result<i64, DataFusionError> {
    use datafusion::parquet::arrow::arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector,
    };
    use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
    use futures::TryStreamExt;

    if row_id < 0 {
        return Err(DataFusionError::Execution(format!(
            "point read: negative row_id {row_id}"
        )));
    }
    let meta = object_metas.first().ok_or_else(|| {
        DataFusionError::Execution("point read: shard view has no parquet file".to_string())
    })?;

    // Footer via the node's file-metadata cache — no per-get footer re-read (that cold read
    // was the throughput regression). Footer-only here; the column-scoped OffsetIndex is
    // attached below (also cached) so the RowSelection can skip to the single target page.
    let metadata_cache = runtime.runtime_env.cache_manager.get_file_metadata_cache();
    let (_schema, _size, pq_meta) =
        crate::indexed_table::parquet_bridge::load_parquet_metadata_with_meta(
            Arc::clone(&store),
            &meta.location,
            meta.clone(),
            metadata_cache,
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("point read: metadata: {e}")))?;

    // Locate (row group, rg-local offset) via a prefix sum over row-group row counts.
    let mut acc: i64 = 0;
    let mut located: Option<(usize, usize)> = None;
    for i in 0..pq_meta.num_row_groups() {
        let n = pq_meta.row_group(i).num_rows();
        if row_id < acc + n {
            located = Some((i, (row_id - acc) as usize));
            break;
        }
        acc += n;
    }
    let (rg, rg_offset) = located.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "point read: row_id {row_id} out of range (file has {acc} rows)"
        ))
    })?;

    // 0.2a — Cached page index: attach a column-scoped OffsetIndex (all projected leaf
    // columns, no predicate ColumnIndex) via the shared, cached loader. With the OffsetIndex
    // present, the RowSelection below skips directly to the page holding the target row per
    // column, instead of decoding every page of the row group. On any failure the loader
    // returns None and we build from the footer alone (decode the whole RG) — never wrong.
    let num_leaf_cols = pq_meta.file_metadata().schema_descr().num_columns();
    let offset_cols: Vec<usize> = (0..num_leaf_cols).collect();
    let augmented = crate::cache::page_index::load_scoped_page_index_cols(
        &store,
        &meta.location,
        &pq_meta,
        &[], // no predicate columns → no (heavy) ColumnIndex; only the cheap OffsetIndex
        &offset_cols,
    )
    .await;
    let arm = match augmented {
        Some(with_index) => {
            ArrowReaderMetadata::try_new(with_index, ArrowReaderOptions::new().with_page_index(true))
        }
        None => ArrowReaderMetadata::try_new(Arc::clone(&pq_meta), ArrowReaderOptions::new()),
    }
    .map_err(|e| DataFusionError::Execution(format!("point read: reader metadata: {e}")))?;

    // 0.2b — Retained local fd: read data byte-ranges from a cached open descriptor (pread)
    // for local files, avoiding a per-get open/stat/close; falls back to the object store for
    // anything that does not resolve to a local file of the expected size.
    let reader = crate::local_file_reader::make_point_reader(
        Arc::clone(&store),
        meta.location.clone(),
        meta.size,
    );
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arm);

    let schema = builder.schema().clone();
    let selection = RowSelection::from(vec![RowSelector::skip(rg_offset), RowSelector::select(1)]);
    let pq_stream = builder
        .with_row_groups(vec![rg])
        .with_row_selection(selection)
        .build()
        .map_err(|e| DataFusionError::Execution(format!("point read: build stream: {e}")))?;

    // B (single-row synchronous delivery): eagerly drain the one-row result here — this runs on
    // the cpu_executor task that invoked us (via df_execute_query's spawn), so decode stays off the
    // IO thread. Then deliver via a PRE-MATERIALIZED CrossRtStream (batch already buffered),
    // avoiding the executor spawn + cross-runtime channel round-trips that dominate `thread_sync`
    // for a 1-row get. No abort handle to register — the read is already complete.
    let _ = &cpu_executor; // materialized path needs no spawn
    let batches = pq_stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| DataFusionError::Execution(format!("point read: collect row: {e}")))?;
    let items = batches.into_iter().map(Ok).collect::<Vec<_>>();
    let cross_rt_stream = CrossRtStream::from_materialized(schema, items);
    let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );
    Ok(Box::into_raw(Box::new(wrapped)) as i64)
}

/// Execute a vanilla parquet query: substrait plan → DataFusion → CrossRtStream.
/// File access goes through DataFusion's registered object store.
///
/// Deprecated: Production now uses the decomposed `create_session_context` +
/// `execute_with_context` path (via `api::execute_query`).
/// TODO: Remove this function and migrate benchmarks to the decomposed path.
/// Retained only for benchmarks. TODO: migrate benchmarks and remove.
pub async fn execute_query(
    table_path: ListingTableUrl,
    object_metas: Arc<Vec<ObjectMeta>>,
    table_name: String,
    plan_bytes: Vec<u8>,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
    query_memory_pool: Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>>,
    query_config: &crate::datafusion_query_config::DatafusionQueryConfig,
    context_id: i64,
    shard_store: Arc<dyn ObjectStore>,
    phantom_corrector: Option<Arc<crate::phantom_corrector::PhantomCorrector>>,
    sort_fields: &[String],
    sort_orders: &[String],
    internal_search: crate::datafusion_query_config::InternalSearch,
) -> Result<i64, DataFusionError> {
    // Fast path: a get-by-`__row_id__` point lookup needs no query engine. Read the single
    // target row directly from parquet, bypassing SessionContext, ListingTable registration,
    // and logical/physical planning (the dominant cost of this path).
    //
    // BENCHMARK WIRING SWITCH (get-by-id A/B):
    //   POINT_READ_MODE = true  → IMPROVED path (`point_read_by_row_id`: cached page index + retained fd).
    //   POINT_READ_MODE = false → BASELINE path (fall through to the full query engine below:
    //                             `SELECT * WHERE __row_id__ = n LIMIT 1` via SessionContext +
    //                             ListingTable + physical planning — the original get-by-id path).
    // Both return the full `_source` (all columns). Flip this const + recompile to compare the two.
    if let crate::datafusion_query_config::InternalSearch::ByRowId(row_id) = &internal_search {
        if POINT_READ_MODE {
            return point_read_by_row_id(
                object_metas.clone(),
                std::sync::Arc::clone(&shard_store),
                cpu_executor.clone(),
                runtime,
                context_id,
                *row_id,
            )
            .await;
        }
        // else: baseline — fall through to the full-query path below (handles ByRowId as SELECT *).
    }

    // Build per-query RuntimeEnv (optional pool overlay) + register the shard store.
    let runtime_env = build_query_runtime_env_with_store(
        runtime,
        &table_path,
        object_metas.as_ref(),
        shard_store,
        query_memory_pool,
    )?;

    // Build a fresh session context per query (default optimizer rules on the
    // vanilla path). TODO : Tune this during planning per query.
    let ctx = build_query_session_context(
        query_config,
        runtime_env,
        query_config.target_partitions,
        false, // vanilla path
    );

    // Register the standard DataFusion ListingTable. This function only runs the vanilla
    // (non-row-id) path — QTF row-id plans always route to the indexed executor.
    // Declares the per-file sort order when the index has `index.sort.field`.
    register_listing_table(&ctx, &table_name, table_path, sort_fields, sort_orders).await?;

    // Planning: build the query DataFrame (Substrait decode for normal search, native filter for an
    // engine-internal point lookup). Physical planning + execution below is shared by both.
    let dataframe = build_dataframe(&ctx, &table_name, &plan_bytes, internal_search).await?;
    let physical_plan = dataframe.create_physical_plan().await?;

    // Retag any physical-plan output columns whose type tags differ from what Substrait
    // declared on bit-compatible Int↔UInt pairs (see crate::relabel_exec). The target is
    // schema_coerce::coerce_inferred_schema(physical_schema) — the same narrowing the
    // partition-stream registration uses, so the consumer's StreamingTable and the
    // batches arriving from this producer agree by construction.
    let target_schema = crate::schema_coerce::coerce_inferred_schema(physical_plan.schema());
    let physical_plan = crate::relabel_exec::wrap_if_relabel_needed(physical_plan, target_schema)?;

    let df_stream = execute_stream(physical_plan, ctx.task_ctx()).map_err(|e| {
        error!("Failed to create execution stream: {}", e);
        e
    })?;

    // Wrap in CrossRtStream — CPU work runs on DedicatedExecutor
    let (cross_rt_stream, abort_handle, _task_done) =
        CrossRtStream::new_with_df_error_stream_cancellable(df_stream, cpu_executor.clone(), None);

    if let Some(h) = abort_handle {
        crate::query_tracker::set_abort_handle(context_id, h);
    }
    if let Some(rt) = cpu_executor.handle() {
        crate::query_tracker::set_cpu_runtime_handle(context_id, rt);
    }

    // Attach phantom corrector for self-correcting budget (if provided)
    let cross_rt_stream = match phantom_corrector {
        Some(corrector) => cross_rt_stream.with_phantom_corrector(corrector),
        None => cross_rt_stream,
    };

    let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );

    Ok(Box::into_raw(Box::new(wrapped)) as i64)
}

/// Build the query DataFrame against the table already registered in `ctx`.
///
/// An engine-internal point lookup (get-by-id / seq-no scan) returns early with a small native
/// filter DataFrame; otherwise this is the standard user-search flow: decode the Substrait plan
/// into a logical plan and execute it.
///
/// Internal-search filters:
/// - [`InternalSearch::ByRowId`]: `SELECT * WHERE __row_id__ = n LIMIT 1`. `__row_id__` is the
///   physical row position the writer stamps at flush, so equality is order-independent and prunes
///   row-groups/pages via min/max stats.
/// - [`InternalSearch::SeqNoAbove`]: `SELECT _id,_seq_no,_primary_term,_version WHERE _seq_no > f`
///   (version-map restore on recovery; metadata columns only).
async fn build_dataframe(
    ctx: &SessionContext,
    table_name: &str,
    plan_bytes: &[u8],
    internal_search: crate::datafusion_query_config::InternalSearch,
) -> Result<datafusion::dataframe::DataFrame, DataFusionError> {
    // Engine-internal point lookup — build a native filter DataFrame and return early.
    if internal_search.is_internal_search() {
        return internal_search_dataframe(ctx, table_name, internal_search).await;
    }

    // Standard user-search flow: Substrait → logical plan → DataFrame.
    let substrait_plan = Plan::decode(plan_bytes)
        .map_err(|e| DataFusionError::Execution(format!("Failed to decode Substrait: {}", e)))?;
    let logical_plan = from_substrait_plan(&ctx.state(), &substrait_plan).await?;
    ctx.execute_logical_plan(logical_plan).await
}

/// Build the native filter DataFrame for an engine-internal point lookup against the table already
/// registered in `ctx`. Not user search — those go through the Substrait flow in [`build_dataframe`].
///
/// - [`InternalSearch::ByRowId`]: `SELECT * WHERE __row_id__ = n LIMIT 1`. `__row_id__` is the
///   physical row position the writer stamps at flush, so equality is order-independent and prunes
///   row-groups/pages via min/max stats.
/// - [`InternalSearch::SeqNoAbove`]: `SELECT _id,_seq_no,_primary_term,_version WHERE _seq_no > f`
///   (version-map restore on recovery; metadata columns only).
///
/// Panics on [`InternalSearch::Off`] — callers gate on `is_internal_search()` first.
async fn internal_search_dataframe(
    ctx: &SessionContext,
    table_name: &str,
    internal_search: crate::datafusion_query_config::InternalSearch,
) -> Result<datafusion::dataframe::DataFrame, DataFusionError> {
    use crate::datafusion_query_config::InternalSearch;
    let df = ctx.table(table_name).await?;
    match internal_search {
        InternalSearch::ByRowId(row_id) => df
            .filter(col(crate::ROW_ID_COLUMN_NAME).eq(lit(row_id)))?
            .limit(0, Some(1)),
        InternalSearch::SeqNoAbove(seq_no_floor) => df
            .filter(col("_seq_no").gt(lit(seq_no_floor)))?
            .select_columns(&["_id", "_seq_no", "_primary_term", "_version"]),
        InternalSearch::Off => unreachable!("internal_search_dataframe called with Off"),
    }
}

/// Executes a Substrait plan against a pre-configured SessionContext.
///
/// Takes ownership of the handle by value. The ownership transfer (consuming the
/// raw Java pointer) happens at the FFM entry in `df_execute_with_context`, so
/// by the time this function is reached the pointer is already invalidated from
/// Java's perspective and cleanup is pure RAII.
///
/// This is the fragment (non-row-id) execution path: row-id-requesting plans are
/// routed to the indexed executor by `df_execute_with_context` before reaching here.
pub async fn execute_with_context(
    handle: SessionContextHandle,
    plan_bytes: &[u8],
    cpu_executor: DedicatedExecutor,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> Result<i64, DataFusionError> {
    // Permit was acquired by the caller (ffm.rs) on the IO runtime before
    // spawning on the CPU runtime, so the Java search thread blocks at the
    // gate when it is full — creating backpressure at the Java threadpool level.
    let context_id = handle.query_context.context_id();
    let token = crate::query_tracker::get_cancellation_token(context_id);

    let query_future = async {
        // If prepare_partial_plan stored a stripped plan on this handle (engine-native-merge
        // PARTIAL stage triggered by SETUP_PARTIAL_AGGREGATE), skip the substrait re-decode
        // and run the prepared plan directly. This activates `force_aggregate_mode(Partial)`
        // semantics — only AggregateExec(Mode::Partial) executes, emitting state-suffixed
        // columns on the wire (e.g. `dc[hll_registers]: Binary` for HLL sketch state). The
        // FINAL substrait declares VARBINARY (resolver's overrideExchangeType), so the wire
        // matches the exchange contract. Non-engine-native paths leave `prepared_plan` as None
        // and fall through to the standard decode + execute below.
        if let Some(prepared) = handle.prepared_plan.as_ref() {
            let physical_plan = std::sync::Arc::clone(prepared);
            let df_stream =
                execute_stream(physical_plan.clone(), handle.ctx.task_ctx()).map_err(|e| {
                    error!(
                        "execute_with_context: failed to execute prepared plan: {}",
                        e
                    );
                    e
                })?;
            let (cross_rt_stream, abort_handle, _task_done) =
                CrossRtStream::new_with_df_error_stream_cancellable(
                    df_stream,
                    cpu_executor.clone(),
                    None,
                );
            if let Some(h) = abort_handle {
                crate::query_tracker::set_abort_handle(context_id, h);
            }
            if let Some(rt) = cpu_executor.handle() {
                crate::query_tracker::set_cpu_runtime_handle(context_id, rt);
            }
            let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                cross_rt_stream.schema(),
                cross_rt_stream,
            );
            return Ok::<
                (
                    i64,
                    Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
                ),
                DataFusionError,
            >((Box::into_raw(Box::new(wrapped)) as i64, Some(physical_plan)));
        }

        let substrait_plan = Plan::decode(plan_bytes).map_err(|e| {
            DataFusionError::Execution(format!("Failed to decode Substrait: {}", e))
        })?;

        // Union schema widening was applied at table registration (session_context::widen_to_union_schema).
        let logical_plan = from_substrait_plan(&handle.ctx.state(), &substrait_plan).await?;
        log_debug!(
            "DataFusion logical plan:\n{}",
            logical_plan.display_indent()
        );

        // Empty shard: skip physical planning (ParquetExec errors on zero files)
        // and emit an EmptyExec stream with the logical plan's output schema.
        //
        // Gate on a non-empty table_name so this fires ONLY for real shard scans. Hash-shuffle
        // WORKER sessions (create_worker_session_context) also carry empty object_metas — they
        // scan registered StreamingTables, not parquet files — but use an empty table_name. Without
        // this guard a worker's join/aggregate plan would short-circuit to EmptyExec and silently
        // return zero rows (regression caught by HashShuffleJoinIT after the #21754 empty-index merge).
        if handle.object_metas.is_empty() && !handle.table_name.is_empty() {
            use datafusion::physical_plan::empty::EmptyExec;
            use datafusion::physical_plan::ExecutionPlan;
            let plan_schema: arrow::datatypes::SchemaRef =
                Arc::new(logical_plan.schema().as_arrow().clone());
            let plan_schema = crate::schema_coerce::coerce_inferred_schema(plan_schema);
            let empty_exec = EmptyExec::new(Arc::clone(&plan_schema));
            let df_stream = empty_exec.execute(0, handle.ctx.task_ctx()).map_err(|e| {
                error!("execute_with_context: failed to create empty stream: {}", e);
                e
            })?;

            let (cross_rt_stream, abort_handle, _task_done) =
                CrossRtStream::new_with_df_error_stream_cancellable(
                    df_stream,
                    cpu_executor.clone(),
                    None,
                );
            if let Some(h) = abort_handle {
                crate::query_tracker::set_abort_handle(context_id, h);
            }
            if let Some(rt) = cpu_executor.handle() {
                crate::query_tracker::set_cpu_runtime_handle(context_id, rt);
            }
            let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                cross_rt_stream.schema(),
                cross_rt_stream,
            );
            return Ok::<
                (
                    i64,
                    Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
                ),
                DataFusionError,
            >((Box::into_raw(Box::new(wrapped)) as i64, None));
        }

        let dataframe = handle.ctx.execute_logical_plan(logical_plan).await?;
        // create_physical_plan runs all registered physical optimizer rules including
        // ProjectRowIdOptimizer (registered in session_context when strategy=ListingTable).
        let physical_plan = dataframe.create_physical_plan().await?;

        let target_schema = crate::schema_coerce::coerce_inferred_schema(physical_plan.schema());
        let physical_plan =
            crate::relabel_exec::wrap_if_relabel_needed(physical_plan, target_schema)?;
        log_debug!(
            "DataFusion physical plan:\n{}",
            displayable(physical_plan.as_ref()).indent(true)
        );

        let df_stream =
            execute_stream(physical_plan.clone(), handle.ctx.task_ctx()).map_err(|e| {
                error!("execute_with_context: failed to create stream: {}", e);
                e
            })?;

        let (cross_rt_stream, abort_handle, _task_done) =
            CrossRtStream::new_with_df_error_stream_cancellable(
                df_stream,
                cpu_executor.clone(),
                None,
            );

        if let Some(h) = abort_handle {
            crate::query_tracker::set_abort_handle(context_id, h);
        }
        if let Some(rt) = cpu_executor.handle() {
            crate::query_tracker::set_cpu_runtime_handle(context_id, rt);
        }

        let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            cross_rt_stream.schema(),
            cross_rt_stream,
        );

        Ok::<
            (
                i64,
                Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
            ),
            DataFusionError,
        >((Box::into_raw(Box::new(wrapped)) as i64, Some(physical_plan)))
    };

    let (stream_ptr, physical_plan) =
        crate::cancellation::cancellable(token.as_ref(), context_id, query_future)
            .await
            .map_err(|e| DataFusionError::Execution(e))?;

    // Reconstruct the stream from the raw pointer
    let stream = unsafe {
        *Box::from_raw(
            stream_ptr
                as *mut datafusion::physical_plan::stream::RecordBatchStreamAdapter<CrossRtStream>,
        )
    };
    // Permit is held until the QueryStreamHandle is dropped (query complete).
    // If cancellation fires → stream drops → handle drops → permit drops → gate releases.
    let stream_handle = match physical_plan {
        Some(plan) => crate::api::QueryStreamHandle::with_physical_plan(
            stream,
            handle.query_context,
            handle.ctx,
            Some(permit),
            plan,
        ),
        None => crate::api::QueryStreamHandle::with_session_context(
            stream,
            handle.query_context,
            handle.ctx,
            Some(permit),
        ),
    };
    Ok(Box::into_raw(Box::new(stream_handle)) as i64)
}

// ── Shared helpers ──────────────────────────────────────────────────────────

/// The shared per-query `RuntimeEnv` builder chain: inherits the global runtime's
/// caches (file-metadata, file-statistics + limit) and uses a fresh object-store
/// registry plus the provided per-query `list_file_cache`. Callers overlay any
/// per-query memory pool, then `.build()`.
pub fn query_runtime_env_builder(
    runtime: &DataFusionRuntime,
    list_file_cache: Arc<DefaultListFilesCache>,
) -> RuntimeEnvBuilder {
    RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
        .with_object_store_registry(Arc::new(
            datafusion::execution::object_store::DefaultObjectStoreRegistry::new(),
        ))
        .with_cache_manager(
            CacheManagerConfig::default()
                .with_list_files_cache(Some(list_file_cache))
                .with_file_metadata_cache(Some(
                    runtime.runtime_env.cache_manager.get_file_metadata_cache(),
                ))
                .with_metadata_cache_limit(
                    runtime.runtime_env.cache_manager.get_metadata_cache_limit(),
                )
                .with_file_statistics_cache(
                    runtime.runtime_env.cache_manager.get_file_statistic_cache(),
                ),
        )
}

/// Build a per-query RuntimeEnv sharing global caches, with a fresh list-files
/// cache pre-populated for the given table path and object metas.
pub fn build_query_runtime_env(
    runtime: &DataFusionRuntime,
    table_path: &ListingTableUrl,
    object_metas: &[ObjectMeta],
) -> Result<Arc<datafusion::execution::runtime_env::RuntimeEnv>, DataFusionError> {
    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    let table_scoped_path = datafusion::execution::cache::TableScopedPath {
        table: None,
        path: table_path.prefix().clone(),
    };
    list_file_cache.put(
        &table_scoped_path,
        CachedFileList::new(object_metas.to_vec()),
    );

    let runtime_env = query_runtime_env_builder(runtime, list_file_cache).build()?;
    Ok(Arc::from(runtime_env))
}

/// Build ShardFileInfo list from object metas by reading parquet footers.
/// Each file gets a cumulative `row_base` and per-RG row counts.
pub async fn build_shard_file_infos(
    store: &Arc<dyn object_store::ObjectStore>,
    object_metas: &[ObjectMeta],
) -> Result<Vec<ShardFileInfo>, DataFusionError> {
    let mut files: Vec<ShardFileInfo> = Vec::new();
    let mut cumulative_rows: i64 = 0;
    for meta in object_metas {
        let reader = datafusion::parquet::arrow::async_reader::ParquetObjectReader::new(
            Arc::clone(store),
            meta.location.clone(),
        )
        .with_file_size(meta.size);
        let builder = datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| DataFusionError::Execution(format!("parquet metadata: {}", e)))?;
        let pq_meta = builder.metadata().clone();
        let num_rows: i64 = (0..pq_meta.num_row_groups())
            .map(|i| pq_meta.row_group(i).num_rows())
            .sum();

        files.push(ShardFileInfo {
            object_meta: meta.clone(),
            row_base: cumulative_rows,
            num_rows: num_rows as u64,
            row_group_row_counts: (0..pq_meta.num_row_groups())
                .map(|i| pq_meta.row_group(i).num_rows() as u64)
                .collect(),
            access_plan: None,
        });
        cumulative_rows += num_rows;
    }
    Ok(files)
}

/// Parse a ListingTableUrl into an ObjectStoreUrl (scheme + authority).
pub fn store_url_from_table_path(
    table_path: &ListingTableUrl,
) -> Result<datafusion::execution::object_store::ObjectStoreUrl, DataFusionError> {
    let url_str = table_path.as_str();
    let parsed = url::Url::parse(url_str)
        .map_err(|e| DataFusionError::Execution(format!("parse URL: {}", e)))?;
    datafusion::execution::object_store::ObjectStoreUrl::parse(format!(
        "{}://{}",
        parsed.scheme(),
        parsed.authority()
    ))
}

/// Wrap a DataFusion stream in CrossRtStream and package as a QueryStreamHandle pointer.
///
/// Wires cancellation like the shard-query path so a `cancel_query` on the QTF fetch-by-rowid
/// stream can break/abort the cross_rt task instead of stranding its pool reservation.
pub fn wrap_stream_as_handle(
    df_stream: datafusion::execution::SendableRecordBatchStream,
    cpu_executor: DedicatedExecutor,
    runtime: &DataFusionRuntime,
    context_id: i64,
) -> i64 {
    wrap_stream_as_handle_with_plan(df_stream, cpu_executor, runtime, context_id, None)
}

/// Like [`wrap_stream_as_handle`] but attaches a physical plan so that execution metrics
/// can be extracted from the plan tree after the stream is exhausted.
pub fn wrap_stream_as_handle_with_plan(
    df_stream: datafusion::execution::SendableRecordBatchStream,
    cpu_executor: DedicatedExecutor,
    runtime: &DataFusionRuntime,
    context_id: i64,
    physical_plan: Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
) -> i64 {
    // Create the tracking context first so its cancellation token is registered before the task starts.
    let query_context = crate::query_tracker::QueryTrackingContext::new(
        context_id,
        runtime.runtime_env.memory_pool.clone(),
        crate::query_tracker::QueryType::Shard,
    );

    let (cross_rt_stream, abort_handle, _task_done) =
        CrossRtStream::new_with_df_error_stream_cancellable(df_stream, cpu_executor.clone(), None);
    if let Some(h) = abort_handle {
        crate::query_tracker::set_abort_handle(context_id, h);
    }
    if let Some(rt) = cpu_executor.handle() {
        crate::query_tracker::set_cpu_runtime_handle(context_id, rt);
    }

    let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );
    let handle = match physical_plan {
        Some(plan) => {
            crate::api::QueryStreamHandle::new_with_plan(wrapped, query_context, None, plan)
        }
        None => crate::api::QueryStreamHandle::new(wrapped, query_context, None),
    };
    Box::into_raw(Box::new(handle)) as i64
}

#[cfg(test)]
mod point_read_pageskip_tests {
    //! Isolates the 0.2a page-skip mechanism: does a single-row `RowSelection` over a
    //! multi-page column actually decode fewer pages? Compares footer-only metadata,
    //! parquet-native page-index metadata, and the scoped `load_scoped_page_index_cols`
    //! path this code uses. Runs without a node (pure parquet round-trip).
    use std::sync::Arc;
    use std::time::Instant;

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector,
    };
    use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::basic::{Compression, ZstdLevel};
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
    use futures::TryStreamExt;
    use object_store::{local::LocalFileSystem, path::Path as ObjPath, ObjectStore};

    async fn time_point_reads(
        store: &Arc<dyn ObjectStore>,
        loc: &ObjPath,
        size: u64,
        arm: &ArrowReaderMetadata,
        n_rows: usize,
        iters: usize,
    ) -> std::time::Duration {
        let t0 = Instant::now();
        for k in 0..iters {
            let row = (k * 2_654_435_761) % n_rows; // scatter across rows
            let reader = ParquetObjectReader::new(Arc::clone(store), loc.clone()).with_file_size(size);
            let stream = datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder::new_with_metadata(
                reader,
                arm.clone(),
            )
            .with_row_groups(vec![0])
            .with_row_selection(RowSelection::from(vec![
                RowSelector::skip(row),
                RowSelector::select(1),
            ]))
            .build()
            .unwrap();
            let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 1, "expected exactly 1 row");
        }
        t0.elapsed()
    }

    #[tokio::test]
    async fn page_skip_reduces_decode_work() {
        let n = 30000usize;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("big", DataType::Utf8, false),
        ]));
        let ids: ArrayRef = Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>()));
        // Distinct, ~incompressible ~1KB values per row → ZSTD can't shrink them, so decoding a
        // page is real CPU work and the column spans ~30 pages. If page-skip works, reading one
        // row decodes 1 page instead of 30 — an unambiguous, decode-dominated difference.
        let mut seed: u64 = 0x9e3779b97f4a7c15;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let big_vals: Vec<String> = (0..n)
            .map(|_| {
                (0..1024)
                    .map(|_| {
                        let r = (next() % 62) as u8;
                        (if r < 26 {
                            b'a' + r
                        } else if r < 52 {
                            b'A' + r - 26
                        } else {
                            b'0' + r - 52
                        }) as char
                    })
                    .collect::<String>()
            })
            .collect();
        let bigs: ArrayRef =
            Arc::new(StringArray::from(big_vals.iter().map(|s| s.as_str()).collect::<Vec<_>>()));
        let batch = RecordBatch::try_new(schema.clone(), vec![ids, bigs]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .set_data_page_size_limit(1024 * 1024)
            .set_data_page_row_count_limit(20000)
            .set_max_row_group_size(1_000_000)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        {
            let file = std::fs::File::create(&path).unwrap();
            let mut w = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        let size = std::fs::metadata(&path).unwrap().len();

        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        // LocalFileSystem::new() is rooted at "/"; object_store Path drops the leading slash.
        let loc = ObjPath::from(path.to_str().unwrap().trim_start_matches('/'));

        // (A) parquet-native metadata WITH page index.
        let mut reader = ParquetObjectReader::new(Arc::clone(&store), loc.clone()).with_file_size(size);
        let arm_pi = ArrowReaderMetadata::load_async(
            &mut reader,
            ArrowReaderOptions::new().with_page_index(true),
        )
        .await
        .unwrap();
        let oi = arm_pi.metadata().offset_index().expect("offset index present");
        let big_pages = oi[0][1].page_locations().len();
        assert!(big_pages > 1, "expected multi-page big col, got {big_pages}");
        println!("big col pages = {big_pages}");

        // (B) footer-only metadata (no page index).
        let mut reader2 = ParquetObjectReader::new(Arc::clone(&store), loc.clone()).with_file_size(size);
        let arm_footer =
            ArrowReaderMetadata::load_async(&mut reader2, ArrowReaderOptions::new()).await.unwrap();

        // (C) the scoped loader path this code uses (footer + grafted OffsetIndex).
        let footer_only = Arc::clone(arm_footer.metadata());
        let ncols = footer_only.file_metadata().schema_descr().num_columns();
        let offset_cols: Vec<usize> = (0..ncols).collect();
        let augmented = crate::cache::page_index::load_scoped_page_index_cols(
            &store, &loc, &footer_only, &[], &offset_cols,
        )
        .await;
        assert!(augmented.is_some(), "scoped loader returned None");
        let augmented = augmented.unwrap();
        assert!(
            augmented.offset_index().is_some(),
            "scoped metadata missing offset index"
        );
        let arm_scoped =
            ArrowReaderMetadata::try_new(augmented, ArrowReaderOptions::new().with_page_index(true))
                .unwrap();

        let iters = 200usize;
        let t_footer = time_point_reads(&store, &loc, size, &arm_footer, n, iters).await;
        let t_pi = time_point_reads(&store, &loc, size, &arm_pi, n, iters).await;
        let t_scoped = time_point_reads(&store, &loc, size, &arm_scoped, n, iters).await;
        // Informational: page-skip should make the indexed variants faster on this many-page,
        // incompressible column. Timing is machine-dependent, so it is printed, not asserted.
        println!(
            "point reads x{iters}: footer_only={t_footer:?}  page_index={t_pi:?}  scoped={t_scoped:?}"
        );
        // Deterministic guarantees (non-flaky): the scoped loader augments the footer with an
        // offset index over a genuinely multi-page column, and each variant returns exactly the
        // one selected row (asserted inside time_point_reads). big_pages > 1 was asserted above.
    }
}
