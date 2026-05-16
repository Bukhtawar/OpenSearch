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

class StopCommand extends JmxHeapProfCommand {

    StopCommand() {
        super("Deactivate jemalloc heap profiling");
    }

    @Override
    protected void invokeOnMBean(MBeanServerConnection mbs, ObjectName mbean, Terminal terminal, OptionSet options) throws Exception {
        mbs.invoke(mbean, "deactivate", null, null);
        terminal.println("Heap profiling deactivated.");
    }
}
