/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.spi;

/**
 * Callback invoked when a pool's limit changes. Consumers that mirror the
 * Java-side pool limit to a separate accountant (e.g., a native runtime that
 * holds its own memory pool) implement this and register it with the
 * allocator so resize events propagate.
 *
 * @opensearch.api
 */
@FunctionalInterface
public interface NativeAllocatorListener {

    /**
     * Invoked after a pool's limit has been updated.
     *
     * @param poolName logical pool name
     * @param newLimit new limit in bytes
     */
    void onPoolLimitChanged(String poolName, long newLimit);
}
