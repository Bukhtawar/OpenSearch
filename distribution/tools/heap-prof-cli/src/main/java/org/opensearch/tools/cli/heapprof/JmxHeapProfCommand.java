/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.heapprof;

import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.opensearch.cli.Command;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * Base class for heap profiling commands that connect to the local OpenSearch
 * JVM via JMX local attach.
 *
 * Connection flow:
 * 1. Find the OpenSearch PID (from --pid option, OPENSEARCH_PID env, or auto-detect)
 * 2. Attach to the target JVM via the Attach API
 * 3. Ensure the JMX local connector agent is running
 * 4. Connect to the MBeanServer
 * 5. Invoke the {@code org.opensearch.native:type=HeapProfiler} MBean
 */
abstract class JmxHeapProfCommand extends Command {

    static final String MBEAN_NAME = "org.opensearch.native:type=HeapProfiler";
    private static final String OPENSEARCH_MAIN_CLASS = "org.opensearch.bootstrap.OpenSearch";

    private final OptionSpec<String> pidOption;

    JmxHeapProfCommand(String description) {
        super(description);
        pidOption = parser.accepts("pid", "OpenSearch process ID (auto-detected if omitted)")
            .withRequiredArg()
            .ofType(String.class);
    }

    protected abstract void invokeOnMBean(MBeanServerConnection mbs, ObjectName mbean, Terminal terminal, OptionSet options)
        throws Exception;

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        String pid = resolvePid(options, terminal);
        terminal.println("Connecting to OpenSearch JVM (PID: " + pid + ")...");

        VirtualMachine vm = VirtualMachine.attach(pid);
        JMXConnector connector = null;
        try {
            JMXServiceURL jmxUrl = getOrStartLocalJmxAgent(vm);
            connector = JMXConnectorFactory.connect(jmxUrl);
            MBeanServerConnection mbs = connector.getMBeanServerConnection();
            ObjectName mbean = new ObjectName(MBEAN_NAME);

            if (!mbs.isRegistered(mbean)) {
                throw new UserException(
                    1,
                    "HeapProfiler MBean not registered. Ensure the native bridge module is loaded "
                        + "and jemalloc profiling is compiled in (release builds only)."
                );
            }

            invokeOnMBean(mbs, mbean, terminal, options);
        } finally {
            if (connector != null) {
                connector.close();
            }
            vm.detach();
        }
    }

    private String resolvePid(OptionSet options, Terminal terminal) throws UserException {
        String pid = pidOption.value(options);
        if (pid != null) {
            return pid;
        }

        String envPid = System.getenv("OPENSEARCH_PID");
        if (envPid != null && !envPid.isEmpty()) {
            terminal.println("Using PID from OPENSEARCH_PID env: " + envPid);
            return envPid;
        }

        List<VirtualMachineDescriptor> vms = VirtualMachine.list();
        for (VirtualMachineDescriptor vmd : vms) {
            if (vmd.displayName().contains(OPENSEARCH_MAIN_CLASS)) {
                terminal.println("Auto-detected OpenSearch process: " + vmd.id());
                return vmd.id();
            }
        }

        throw new UserException(
            1,
            "Cannot find running OpenSearch process. Use --pid <PID> or set OPENSEARCH_PID env variable."
        );
    }

    private JMXServiceURL getOrStartLocalJmxAgent(VirtualMachine vm) throws Exception {
        Properties props = vm.getAgentProperties();
        String connectorAddr = props.getProperty("com.sun.management.jmxremote.localConnectorAddress");

        if (connectorAddr == null) {
            vm.startLocalManagementAgent();
            connectorAddr = vm.getAgentProperties().getProperty("com.sun.management.jmxremote.localConnectorAddress");
        }

        if (connectorAddr == null) {
            throw new IOException("Failed to start JMX local management agent on target JVM");
        }

        return new JMXServiceURL(connectorAddr);
    }
}
