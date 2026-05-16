/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.heapprof;

import org.opensearch.cli.MultiCommand;
import org.opensearch.cli.Terminal;

/**
 * CLI tool for on-demand jemalloc heap profiling of the native layer.
 *
 * Connects to the running OpenSearch JVM on the local node via JMX and
 * invokes heap profiling operations directly — no REST API or cluster
 * settings required.
 *
 * Usage:
 *   opensearch-heap-prof start                - Activate heap profiling
 *   opensearch-heap-prof stop                 - Deactivate heap profiling
 *   opensearch-heap-prof dump &lt;path&gt;    - Dump heap profile to file
 *   opensearch-heap-prof reset [lg_sample]    - Reset state (default lg_sample=17)
 *   opensearch-heap-prof status               - Show current profiling status
 */
public class HeapProfCli extends MultiCommand {

    private HeapProfCli() {
        super("Native heap profiling tool for jemalloc (via JMX)", () -> {});
        subcommands.put("start", new StartCommand());
        subcommands.put("stop", new StopCommand());
        subcommands.put("dump", new DumpCommand());
        subcommands.put("reset", new ResetCommand());
        subcommands.put("status", new StatusCommand());
    }

    public static void main(String[] args) throws Exception {
        exit(new HeapProfCli().main(args, Terminal.DEFAULT));
    }
}
