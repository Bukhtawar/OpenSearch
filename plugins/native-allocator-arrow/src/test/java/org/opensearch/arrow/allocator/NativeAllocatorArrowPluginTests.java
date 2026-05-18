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
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.test.OpenSearchTestCase;

public class NativeAllocatorArrowPluginTests extends OpenSearchTestCase {

    public void testResolveRootLimitExplicitSetting() {
        Settings settings = Settings.builder().put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 8L * 1024 * 1024 * 1024).build();
        assertEquals(8L * 1024 * 1024 * 1024, NativeAllocatorArrowPlugin.resolveRootLimit(settings));
    }

    public void testResolveRootLimitDerivedFromAdmissionControl() {
        // root.limit unset (defaults to MAX_VALUE), node.native_memory.limit = 10 GB.
        // Expect derived value = 10 GB × 0.8 = 8 GB.
        Settings settings = Settings.builder()
            .put(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "10gb")
            .build();
        long resolved = NativeAllocatorArrowPlugin.resolveRootLimit(settings);
        long expected = (long) (10L * 1024 * 1024 * 1024 * NativeAllocatorArrowPlugin.AC_DERIVED_FRACTION);
        assertEquals(expected, resolved);
    }

    public void testResolveRootLimitBothUnsetReturnsMax() {
        // Neither root.limit nor node.native_memory.limit set → no enforcement.
        Settings settings = Settings.EMPTY;
        assertEquals(Long.MAX_VALUE, NativeAllocatorArrowPlugin.resolveRootLimit(settings));
    }

    public void testResolveRootLimitExplicitOverridesAcLimit() {
        // Both set: explicit wins.
        Settings settings = Settings.builder()
            .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 4L * 1024 * 1024 * 1024)
            .put(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "10gb")
            .build();
        assertEquals(4L * 1024 * 1024 * 1024, NativeAllocatorArrowPlugin.resolveRootLimit(settings));
    }
}
