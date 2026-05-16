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

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * MBean implementation for jemalloc heap profiling.
 * Delegates to {@link NativeHeapProfiler} for the actual FFM calls.
 *
 * <p>Intended for local access only via the {@code opensearch-heap-prof} CLI tool,
 * which uses {@code VirtualMachine.attach(pid)} — a same-user, local-only mechanism.
 * No network port is opened by this registration.
 *
 * <p>This MBean is NOT reachable from HTTP (9200) or transport (9300) — there is no
 * REST action or transport handler wired to it.
 */
public class HeapProfiler implements HeapProfilerMBean {

    private static final Logger logger = LogManager.getLogger(HeapProfiler.class);
    private static final String OBJECT_NAME = "org.opensearch.native:type=HeapProfiler";

    private final AtomicBoolean active = new AtomicBoolean(false);
    private final AtomicLong lgProfSample = new AtomicLong(17);

    /**
     * Registers this MBean with the platform MBeanServer.
     * Called once during NativeBridgeModule initialization.
     */
    public static HeapProfiler register() {
        HeapProfiler instance = new HeapProfiler();
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName(OBJECT_NAME);
            if (!mbs.isRegistered(name)) {
                mbs.registerMBean(instance, name);
                logger.info("HeapProfiler MBean registered: {}", OBJECT_NAME);
            }
        } catch (Exception e) {
            logger.warn("Failed to register HeapProfiler MBean", e);
        }
        return instance;
    }

    @Override
    public void activate() {
        NativeHeapProfiler.setActive(true);
        active.set(true);
        logger.info("Heap profiling activated via JMX");
    }

    @Override
    public void deactivate() {
        NativeHeapProfiler.setActive(false);
        active.set(false);
        logger.info("Heap profiling deactivated via JMX");
    }

    @Override
    public void dump(String path) {
        logger.info("Heap profile dump requested via JMX: {}", path);
        NativeHeapProfiler.dumpProfile(path);
    }

    @Override
    public void reset(long lgSample) {
        logger.info("Heap profiling reset via JMX: lg_sample={}", lgSample);
        NativeHeapProfiler.resetProfiling(lgSample);
        lgProfSample.set(lgSample);
    }

    @Override
    public boolean isActive() {
        return active.get();
    }

    @Override
    public long getLgProfSample() {
        return lgProfSample.get();
    }
}
