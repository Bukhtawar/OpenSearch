/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.indices.breaker.BreakerSettings;
import org.opensearch.plugins.CircuitBreakerPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Top-level plugin that owns the unified Arrow-backed native memory allocator.
 *
 * <p>All Arrow-consuming plugins (arrow-flight-rpc, parquet-data-format) extend
 * this plugin to share one {@link ArrowNativeAllocator} and its classloader.
 *
 * <p>Each pool has a min (guaranteed floor) and max (burst ceiling). The rebalancer
 * ensures every pool can always allocate up to its min, and distributes unused
 * capacity allowing pools to grow up to their max.
 */
public class NativeAllocatorArrowPlugin extends Plugin implements CircuitBreakerPlugin {

    private static final Logger logger = LogManager.getLogger(NativeAllocatorArrowPlugin.class);

    /**
     * Fraction of the admission-control native-memory budget used as the default
     * unified-pool root limit when {@code native.allocator.root.limit} is unset.
     * The remaining 20% is headroom for non-Arrow native usage that admission control
     * still needs to track (jemalloc fragmentation, parquet-rs scratch, Netty direct,
     * JNI). Without this headroom, the unified pool would saturate the AC budget and
     * leave AC with nothing to throttle on.
     */
    static final double AC_DERIVED_FRACTION = 0.8;

    /** Name of the circuit breaker registered for the unified Arrow native allocator. */
    public static final String BREAKER_NAME = "native_arrow";

    /** Creates the plugin. */
    public NativeAllocatorArrowPlugin() {}

    /** Maximum bytes for the root Arrow allocator. */
    public static final Setting<Long> ROOT_LIMIT_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT,
        Long.MAX_VALUE,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Minimum guaranteed bytes for the Flight pool. */
    public static final Setting<Long> FLIGHT_MIN_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_FLIGHT_MIN,
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Maximum bytes the Flight pool can burst to. */
    public static final Setting<Long> FLIGHT_MAX_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX,
        Long.MAX_VALUE,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Minimum guaranteed bytes for the ingest pool. */
    public static final Setting<Long> INGEST_MIN_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_INGEST_MIN,
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Maximum bytes the ingest pool can burst to. */
    public static final Setting<Long> INGEST_MAX_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_INGEST_MAX,
        Long.MAX_VALUE,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Minimum guaranteed bytes for the query pool. */
    public static final Setting<Long> QUERY_MIN_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_QUERY_MIN,
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Maximum bytes the query pool can burst to. */
    public static final Setting<Long> QUERY_MAX_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_QUERY_MAX,
        Long.MAX_VALUE,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Minimum guaranteed bytes for the DataFusion pool. */
    public static final Setting<Long> DATAFUSION_MIN_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_DATAFUSION_MIN,
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Maximum bytes the DataFusion pool can burst to. */
    public static final Setting<Long> DATAFUSION_MAX_SETTING = Setting.longSetting(
        NativeAllocatorPoolConfig.SETTING_DATAFUSION_MAX,
        Long.MAX_VALUE,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Interval in seconds between pool rebalance cycles. 0 disables rebalancing. */
    public static final Setting<Long> REBALANCE_INTERVAL_SETTING = Setting.longSetting(
        "native.allocator.rebalance.interval_seconds",
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile ArrowNativeAllocator allocator;

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        Settings settings = environment.settings();

        long rootLimit = resolveRootLimit(settings);
        allocator = new ArrowNativeAllocator(rootLimit);
        if (pendingBreaker != null) {
            allocator.setBreaker(pendingBreaker);
            pendingBreaker = null;
        }
        allocator.setRebalanceInterval(REBALANCE_INTERVAL_SETTING.get(settings));

        long flightMin = FLIGHT_MIN_SETTING.get(settings);
        long flightMax = FLIGHT_MAX_SETTING.get(settings);
        long ingestMin = INGEST_MIN_SETTING.get(settings);
        long ingestMax = INGEST_MAX_SETTING.get(settings);
        long queryMin = QUERY_MIN_SETTING.get(settings);
        long queryMax = QUERY_MAX_SETTING.get(settings);
        long datafusionMin = DATAFUSION_MIN_SETTING.get(settings);
        long datafusionMax = DATAFUSION_MAX_SETTING.get(settings);

        validateMinMax(NativeAllocatorPoolConfig.POOL_FLIGHT, flightMin, flightMax);
        validateMinMax(NativeAllocatorPoolConfig.POOL_INGEST, ingestMin, ingestMax);
        validateMinMax(NativeAllocatorPoolConfig.POOL_QUERY, queryMin, queryMax);
        validateMinMax(NativeAllocatorPoolConfig.POOL_DATAFUSION, datafusionMin, datafusionMax);
        validateMinSum(rootLimit, flightMin, ingestMin, queryMin, datafusionMin);

        allocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_FLIGHT, flightMin, flightMax);
        allocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_INGEST, ingestMin, ingestMax);
        allocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_QUERY, queryMin, queryMax);
        allocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_DATAFUSION, datafusionMin, datafusionMax);

        // Seed pool capacity from min toward max via one rebalance pass. Without this, pools
        // sit at min until the rebalancer first runs — and the rebalancer is disabled by
        // default (interval = 0), which would leave consumers with no allocation budget.
        allocator.rebalance();

        clusterService.getClusterSettings().addSettingsUpdateConsumer(ROOT_LIMIT_SETTING, allocator::setRootLimit);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REBALANCE_INTERVAL_SETTING, allocator::setRebalanceInterval);

        return List.of(allocator);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            ROOT_LIMIT_SETTING,
            FLIGHT_MIN_SETTING,
            FLIGHT_MAX_SETTING,
            INGEST_MIN_SETTING,
            INGEST_MAX_SETTING,
            QUERY_MIN_SETTING,
            QUERY_MAX_SETTING,
            DATAFUSION_MIN_SETTING,
            DATAFUSION_MAX_SETTING,
            REBALANCE_INTERVAL_SETTING
        );
    }

    /**
     * Returns the root allocator limit. If {@code native.allocator.root.limit} is unset
     * (defaults to {@code Long.MAX_VALUE}), derives the limit from
     * {@code node.native_memory.limit} (used by admission control) scaled by
     * {@link #AC_DERIVED_FRACTION} so non-Arrow native usage retains headroom.
     * If both settings are unset, returns {@code Long.MAX_VALUE} (no enforcement).
     */
    static long resolveRootLimit(Settings settings) {
        long explicit = ROOT_LIMIT_SETTING.get(settings);
        if (explicit != Long.MAX_VALUE) {
            return explicit;
        }
        long acLimit = ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(settings).getBytes();
        if (acLimit > 0) {
            long derived = (long) (acLimit * AC_DERIVED_FRACTION);
            logger.info(
                "{} unset; deriving from {} ({} bytes) × {} = {} bytes",
                NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT,
                ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(),
                acLimit,
                AC_DERIVED_FRACTION,
                derived
            );
            return derived;
        }
        return Long.MAX_VALUE;
    }

    private static void validateMinMax(String poolName, long min, long max) {
        if (min > max) {
            throw new IllegalArgumentException("Pool '" + poolName + "' min (" + min + ") exceeds max (" + max + ")");
        }
    }

    private static void validateMinSum(long rootLimit, long... mins) {
        if (rootLimit == Long.MAX_VALUE) {
            return;
        }
        long sum = 0;
        for (long min : mins) {
            long prev = sum;
            sum += min;
            if (sum < prev) {
                throw new IllegalArgumentException("Sum of pool minimums overflows.");
            }
        }
        if (sum > rootLimit) {
            throw new IllegalArgumentException(
                "Sum of pool minimums ("
                    + sum
                    + " bytes) exceeds root limit ("
                    + rootLimit
                    + " bytes). "
                    + "Reduce pool minimums or increase "
                    + NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT
            );
        }
    }

    @Override
    public BreakerSettings getCircuitBreaker(Settings settings) {
        long limit = resolveRootLimit(settings);
        return new BreakerSettings(BREAKER_NAME, limit, 1.0d, CircuitBreaker.Type.MEMORY, CircuitBreaker.Durability.PERMANENT);
    }

    @Override
    public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
        // Snapshot the breaker; the allocator instance receives it once createComponents has run.
        // The allocator periodically syncs its total allocation into the breaker's counter so
        // _nodes/stats?breaker reflects native usage and the parent breaker's heap-only tally
        // gains visibility into off-heap pressure.
        ArrowNativeAllocator alloc = allocator;
        if (alloc != null) {
            alloc.setBreaker(circuitBreaker);
        } else {
            // setCircuitBreaker may run before createComponents; stash on the plugin and let
            // createComponents pick it up.
            this.pendingBreaker = circuitBreaker;
        }
    }

    private volatile CircuitBreaker pendingBreaker;

    @Override
    public void close() throws IOException {
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }
}
