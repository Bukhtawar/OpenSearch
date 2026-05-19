/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowBasePluginTests extends OpenSearchTestCase {

    public void testDeriveRootLimitDefaultUnsetReturnsLongMaxValue() {
        Settings s = Settings.EMPTY;
        assertEquals(Long.toString(Long.MAX_VALUE), ArrowBasePlugin.deriveRootLimitDefault(s));
    }

    public void testDeriveRootLimitDefaultUsesAcLimitWhenSet() {
        Settings s = Settings.builder().put("node.native_memory.limit", "1gb").build();
        // 1 GiB == 2^30 bytes; buffer_percent default 0 => full budget.
        assertEquals(Long.toString(1024L * 1024 * 1024), ArrowBasePlugin.deriveRootLimitDefault(s));
    }

    public void testDeriveRootLimitDefaultAppliesBufferPercent() {
        Settings s = Settings.builder()
            .put("node.native_memory.limit", "1000b")
            .put("node.native_memory.buffer_percent", 20)
            .build();
        // 1000 - (1000 * 20 / 100) = 800
        assertEquals("800", ArrowBasePlugin.deriveRootLimitDefault(s));
    }

    public void testRootLimitSettingExposesDerivedDefault() {
        Settings s = Settings.builder().put("node.native_memory.limit", "2gb").build();
        assertEquals(Long.valueOf(2L * 1024 * 1024 * 1024), ArrowBasePlugin.ROOT_LIMIT_SETTING.get(s));
    }

    public void testRootLimitSettingExplicitOverridesDerived() {
        Settings s = Settings.builder()
            .put("node.native_memory.limit", "8gb")
            .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 1024L)
            .build();
        assertEquals(Long.valueOf(1024L), ArrowBasePlugin.ROOT_LIMIT_SETTING.get(s));
    }

    public void testRootLimitRejectsNegative() {
        Settings s = Settings.builder().put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, -1L).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ArrowBasePlugin.ROOT_LIMIT_SETTING.get(s));
        assertTrue(e.getMessage().contains("must be >= 0"));
    }

    public void testQueryAndDataFusionSettingsExposeDefaults() {
        Settings s = Settings.EMPTY;
        assertEquals(Long.valueOf(0L), ArrowBasePlugin.QUERY_MIN_SETTING.get(s));
        assertEquals(Long.valueOf(Long.MAX_VALUE), ArrowBasePlugin.QUERY_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(0L), ArrowBasePlugin.DATAFUSION_MIN_SETTING.get(s));
        assertEquals(Long.valueOf(Long.MAX_VALUE), ArrowBasePlugin.DATAFUSION_MAX_SETTING.get(s));
    }

    public void testFlightAndIngestMinDefaultsToZero() {
        // The grouped validator (validateMinSum) treats per-pool mins as a guarantee
        // floor — defaults of Long.MAX_VALUE caused the validator to reject any PUT
        // that set a non-MAX root. The four pool mins must default to zero so the
        // baseline configuration is consistent.
        Settings s = Settings.EMPTY;
        assertEquals(Long.valueOf(0L), ArrowBasePlugin.FLIGHT_MIN_SETTING.get(s));
        assertEquals(Long.valueOf(0L), ArrowBasePlugin.INGEST_MIN_SETTING.get(s));
    }

    public void testQueryAndDataFusionSettingsAcceptValues() {
        Settings s = Settings.builder()
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MIN, 100L)
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, 1000L)
            .put(NativeAllocatorPoolConfig.SETTING_DATAFUSION_MIN, 200L)
            .put(NativeAllocatorPoolConfig.SETTING_DATAFUSION_MAX, 2000L)
            .build();
        assertEquals(Long.valueOf(100L), ArrowBasePlugin.QUERY_MIN_SETTING.get(s));
        assertEquals(Long.valueOf(1000L), ArrowBasePlugin.QUERY_MAX_SETTING.get(s));
        assertEquals(Long.valueOf(200L), ArrowBasePlugin.DATAFUSION_MIN_SETTING.get(s));
        assertEquals(Long.valueOf(2000L), ArrowBasePlugin.DATAFUSION_MAX_SETTING.get(s));
    }
}
