/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * Starts the native heap profiling Unix domain socket listener.
 * <p>
 * The listener accepts commands (activate, deactivate, dump, reset, status) via a UDS,
 * providing a jcmd-equivalent interface for native heap profiling that works even when
 * the node cannot join the cluster.
 * <p>
 * The CLI tool {@code bin/opensearch-heap-prof} connects to this socket.
 */
public final class NativeHeapProfiler {

    private static final Logger logger = LogManager.getLogger(NativeHeapProfiler.class);

    private static final MethodHandle START_LISTENER;

    static {
        SymbolLookup lookup = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();

        FunctionDescriptor ptrToLong = FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS);
        START_LISTENER = linker.downcallHandle(
            lookup.find("native_jemalloc_heap_prof_start_listener").orElseThrow(), ptrToLong
        );
    }

    private NativeHeapProfiler() {}

    /**
     * Starts the heap profiling UDS listener at the specified socket path.
     * Called once during module initialization.
     *
     * @param socketPath path for the Unix domain socket (e.g., "/data/heap-prof.sock")
     */
    public static void startListener(String socketPath) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cPath = arena.allocateFrom(socketPath);
            long rc = (long) START_LISTENER.invokeExact(cPath);
            NativeLibraryLoader.checkResult(rc);
            logger.info("Native heap profiling listener started at {}", socketPath);
        } catch (Throwable t) {
            logger.warn("Failed to start heap profiling listener at " + socketPath, t);
        }
    }
}
