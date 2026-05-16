/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

/**
 * MBean interface for jemalloc heap profiling operations.
 * Registered as {@code org.opensearch.native:type=HeapProfiler}.
 *
 * Enables the {@code opensearch-heap-prof} CLI tool to control profiling
 * on a running node via JMX local attach without cluster settings.
 */
public interface HeapProfilerMBean {

    /**
     * Activates jemalloc heap profiling. Allocations are sampled from this point.
     */
    void activate();

    /**
     * Deactivates jemalloc heap profiling. Sampling stops.
     */
    void deactivate();

    /**
     * Dumps the current heap profile to the specified file path.
     *
     * @param path absolute file path for the heap dump output
     */
    void dump(String path);

    /**
     * Resets accumulated profiling data and sets a new sample interval.
     * WARNING: discards all existing profiling data. Dump first if needed.
     *
     * @param lgSample log2 of sample interval in bytes (e.g., 17 = ~128KB)
     */
    void reset(long lgSample);

    /**
     * Returns whether heap profiling is currently active.
     */
    boolean isActive();

    /**
     * Returns the current lg_prof_sample value, or -1 if unknown.
     */
    long getLgProfSample();
}
