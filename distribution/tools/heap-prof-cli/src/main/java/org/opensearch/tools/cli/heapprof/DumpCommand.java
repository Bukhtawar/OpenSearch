/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.heapprof;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

class DumpCommand extends JmxHeapProfCommand {

    private final OptionSpec<String> pathArg;

    DumpCommand() {
        super("Dump heap profile to a file");
        pathArg = parser.nonOptions("output file path").ofType(String.class);
    }

    @Override
    protected void invokeOnMBean(MBeanServerConnection mbs, ObjectName mbean, Terminal terminal, OptionSet options) throws Exception {
        String path = pathArg.value(options);
        if (path == null || path.isEmpty()) {
            throw new UserException(1, "Output path required. Usage: opensearch-heap-prof dump /path/to/output.heap");
        }

        mbs.invoke(mbean, "dump", new Object[] { path }, new String[] { String.class.getName() });
        terminal.println("Heap profile dumped to: " + path);
        terminal.println("Analyze with:");
        terminal.println("  jeprof --text --lines libopensearch_native.so " + path + " | rustfilt");
    }
}
