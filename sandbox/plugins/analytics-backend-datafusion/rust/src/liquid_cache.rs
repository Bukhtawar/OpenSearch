/* SPDX-License-Identifier: Apache-2.0 */

use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock,
    },
};

use arrow_schema::DataType;
use datafusion::{
    common::{config::ConfigOptions, DataFusionError},
    optimizer::OptimizerRule,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::ExecutionPlan,
    prelude::SessionConfig,
};

use liquid_cache_datafusion_local::{
    storage::cache::{squeeze_policies::TranscodeSqueezeEvict, CachePolicy, LiquidCache, NoHydration},
    storage::cache_policies::{LiquidPolicy, LruPolicy},
    LiquidCacheLocalBuilder,
};
use native_bridge_common::{log_debug, log_info};

const LOCAL_MODE_OPTIMIZER_NAME: &str = "LocalModeLiquidCacheOptimizer";
const LINEAGE_OPTIMIZER_NAME: &str = "LineageOptimizer";
const EVICTION_POLICY_LRU: &str = "lru";
const CACHE_DIR_PREFIX: &str = "node_";

static INSTANCE: OnceLock<Result<LiquidOnlyRuntime, String>> = OnceLock::new();

pub struct LiquidOnlyRuntime {
    optimizer: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    lineage_optimizer: Arc<dyn OptimizerRule + Send + Sync>,
    _handle: Box<dyn std::any::Any + Send + Sync>,
    storage: Arc<LiquidCache>,
    cache_dir: PathBuf,
    enabled: AtomicBool,
}

impl LiquidOnlyRuntime {
    pub fn init(
        max_cache_bytes: u64,
        max_disk_bytes: u64,
        cache_dir: &str,
        eviction_policy: &str,
        tokio_handle: &tokio::runtime::Handle,
    ) -> Result<&'static Self, DataFusionError> {
        INSTANCE
            .get_or_init(|| Self::build(max_cache_bytes, max_disk_bytes, cache_dir, eviction_policy, tokio_handle))
            .as_ref()
            .map_err(|e| DataFusionError::Execution(e.clone()))
    }

    fn build(
        max_cache_bytes: u64,
        max_disk_bytes: u64,
        cache_dir: &str,
        eviction_policy: &str,
        tokio_handle: &tokio::runtime::Handle,
    ) -> Result<Self, String> {
        let cache_dir = PathBuf::from(cache_dir).join(format!("{}{}", CACHE_DIR_PREFIX, std::process::id()));
        fs::create_dir_all(&cache_dir)
            .map_err(|e| format!("Failed to create cache directory {:?}: {}", cache_dir, e))?;

        let policy: Box<dyn CachePolicy> = match eviction_policy {
            EVICTION_POLICY_LRU => Box::new(LruPolicy::new()),
            _ => Box::new(LiquidPolicy::new()),
        };

        let builder = LiquidCacheLocalBuilder::new()
            .with_max_memory_bytes(max_cache_bytes as usize)
            .with_max_disk_bytes(max_disk_bytes as usize)
            .with_cache_dir(cache_dir.clone())
            .with_cache_policy(policy)
            .with_squeeze_policy(Box::new(TranscodeSqueezeEvict))
            .with_hydration_policy(Box::new(NoHydration::new()));

        let (ctx, cache_ref) = tokio_handle
            .block_on(builder.build(SessionConfig::new()))
            .map_err(|e| format!("Failed to build liquid cache: {}", e))?;

        let storage = cache_ref.storage().clone();
        let state = ctx.state();

        let optimizer = state
            .physical_optimizers()
            .iter()
            .find(|r| r.name() == LOCAL_MODE_OPTIMIZER_NAME)
            .cloned()
            .ok_or_else(|| format!("{} not found in session state", LOCAL_MODE_OPTIMIZER_NAME))?;

        let lineage_optimizer = state
            .optimizers()
            .iter()
            .find(|r| r.name() == LINEAGE_OPTIMIZER_NAME)
            .cloned()
            .ok_or_else(|| format!("{} not found in session state", LINEAGE_OPTIMIZER_NAME))?;

        Ok(Self {
            optimizer,
            lineage_optimizer,
            _handle: Box::new(cache_ref),
            storage,
            cache_dir,
            enabled: AtomicBool::new(true),
        })
    }

    pub fn optimizer(&self) -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
        Arc::new(NumericFilterOnlyLiquidCacheOptimizer::new(self.optimizer.clone()))
    }

    pub fn lineage_optimizer(&self) -> Arc<dyn OptimizerRule + Send + Sync> {
        self.lineage_optimizer.clone()
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn set_max_memory_bytes(&self, bytes: usize) {
        self.storage.budget().set_max_memory_bytes(bytes);
    }

    pub fn set_max_disk_bytes(&self, bytes: usize) {
        self.storage.budget().set_max_disk_bytes(bytes);
    }

    pub fn reset_cache(&self) {
        self.storage.reset();
        self.recreate_cache_dir();
        log_info!("[LiquidCache] Cache cleared");
        self.log_stats();
    }

    pub fn log_stats(&self) {
        let s = self.storage.stats();
        log_debug!(
            "[LiquidCache] entries={}, mem={}/{}, disk={}/{}, \
             arrow={}({} B), liquid={}({} B), squeezed={}({} B), \
             disk_liquid={}, disk_arrow={}",
            s.total_entries,
            s.memory_usage_bytes, s.max_memory_bytes,
            s.disk_usage_bytes, s.max_disk_bytes,
            s.memory_arrow_entries, s.memory_arrow_bytes,
            s.memory_liquid_entries, s.memory_liquid_bytes,
            s.memory_squeezed_liquid_entries, s.memory_squeezed_liquid_bytes,
            s.disk_liquid_entries, s.disk_arrow_entries,
        );
        log_debug!(
            "[LiquidCache] hits={}, misses={}, predicate_evals={}, \
             squeeze_ok={}, squeeze_io={}, read_io={}, write_io={}, \
             disk_evict={}, squeeze_saved={}",
            s.runtime.cache_hit, s.runtime.cache_miss,
            s.runtime.eval_predicate,
            s.runtime.get_squeezed_success, s.runtime.get_squeezed_needs_io,
            s.runtime.read_io_count, s.runtime.write_io_count,
            s.runtime.disk_evictions, s.runtime.squeeze_io_saved,
        );
    }

    fn recreate_cache_dir(&self) {
        if self.cache_dir.exists() {
            if let Err(e) = fs::remove_dir_all(&self.cache_dir) {
                log_info!("[LiquidCache] Failed to remove cache dir: {}", e);
                return;
            }
        }
        if let Err(e) = fs::create_dir_all(&self.cache_dir) {
            log_info!("[LiquidCache] Failed to recreate cache dir: {}", e);
        }
    }

    fn get() -> Option<&'static Self> {
        INSTANCE.get().and_then(|r| r.as_ref().ok())
    }

    pub fn is_enabled_globally() -> bool {
        Self::get().map(|rt| rt.is_enabled()).unwrap_or(false)
    }

    pub fn set_enabled_globally(enabled: bool) {
        if let Some(rt) = Self::get() {
            rt.set_enabled(enabled);
        }
    }

    pub fn set_max_memory_bytes_globally(bytes: usize) {
        if let Some(rt) = Self::get() {
            rt.set_max_memory_bytes(bytes);
        }
    }

    pub fn set_max_disk_bytes_globally(bytes: usize) {
        if let Some(rt) = Self::get() {
            rt.set_max_disk_bytes(bytes);
        }
    }

    pub fn log_stats_if_initialized() {
        if let Some(rt) = Self::get() {
            rt.log_stats();
        }
    }

    pub fn reset_cache_if_initialized() {
        if let Some(rt) = Self::get() {
            rt.reset_cache();
        }
    }
}

/// Wraps the Liquid Cache physical optimizer, applying it only when the query's
/// filter predicate references exclusively numeric/temporal columns. Queries filtering
/// on string/text columns bypass the cache since those columns are better served by
/// the Lucene inverted index path and would waste cache budget on large string data.
#[derive(Debug)]
pub struct NumericFilterOnlyLiquidCacheOptimizer {
    inner: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
}

impl NumericFilterOnlyLiquidCacheOptimizer {
    pub fn new(inner: Arc<dyn PhysicalOptimizerRule + Send + Sync>) -> Self {
        Self { inner }
    }
}

impl PhysicalOptimizerRule for NumericFilterOnlyLiquidCacheOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if filter_columns_are_cacheable(&plan) {
            self.inner.optimize(plan, config)
        } else {
            log_debug!("[LiquidCache] Skipping — filter references non-numeric columns");
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "NumericFilterOnlyLiquidCacheOptimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Walks the plan tree to find filter predicates. Returns true if ALL filter
/// columns are numeric/temporal (cacheable), or if there's no filter at all.
fn filter_columns_are_cacheable(plan: &Arc<dyn ExecutionPlan>) -> bool {
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_expr::utils::collect_columns;

    // Check this node
    if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
        let predicate = filter.predicate();
        let columns = collect_columns(predicate);
        let schema = filter.children()[0].schema();
        for col in &columns {
            if let Ok(field) = schema.field_with_name(col.name()) {
                if !is_cacheable_type(field.data_type()) {
                    return false;
                }
            }
        }
    }

    // Recurse into children
    for child in plan.children() {
        if !filter_columns_are_cacheable(child) {
            return false;
        }
    }
    true
}

fn is_cacheable_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Boolean
    )
}
