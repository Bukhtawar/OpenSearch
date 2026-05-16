/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.heapprof;

import joptsimple.OptionSet;

import org.opensearch.cli.Terminal;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

class StatusCommand extends JmxHeapProfCommand {

    StatusCommand() {
        super("Show current heap profiling status");
    }

    @Override
    protected void invokeOnMBean(MBeanServerConnection mbs, ObjectName mbean, Terminal terminal, OptionSet options) throws Exception {
        Boolean active = (Boolean) mbs.getAttribute(mbean, "Active");
        Long lgSample = (Long) mbs.getAttribute(mbean, "LgProfSample");

        terminal.println("Heap profiling status:");
        terminal.println("  Active:      " + (Boolean.TRUE.equals(active) ? "YES (sampling)" : "NO (inactive)"));
        if (lgSample != null && lgSample >= 0) {
            terminal.println("  Sample rate: lg_sample=" + lgSample + " (~" + ((1L << lgSample) / 1024) + "KB between samples)");
        }
    }
}
