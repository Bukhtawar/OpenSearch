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

class ResetCommand extends JmxHeapProfCommand {

    private final OptionSpec<String> lgSampleArg;

    ResetCommand() {
        super("Reset profiling state (discards data) and set sample interval");
        lgSampleArg = parser.nonOptions("lg_prof_sample (log2 of bytes between samples, default 17 = ~128KB)").ofType(String.class);
    }

    @Override
    protected void invokeOnMBean(MBeanServerConnection mbs, ObjectName mbean, Terminal terminal, OptionSet options) throws Exception {
        String lgStr = lgSampleArg.value(options);
        long lgSample = 17;
        if (lgStr != null && !lgStr.isEmpty()) {
            lgSample = Long.parseLong(lgStr);
            if (lgSample < 0 || lgSample > 30) {
                throw new UserException(1, "lg_sample must be between 0 and 30, got: " + lgSample);
            }
        }

        mbs.invoke(mbean, "reset", new Object[] { lgSample }, new String[] { long.class.getName() });
        terminal.println("Profiling state reset. New sample interval: ~" + ((1L << lgSample) / 1024) + "KB (lg_sample=" + lgSample + ")");
    }
}
