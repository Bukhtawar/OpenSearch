/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.shard.ReplicationGroup;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Used for performing any replication operation on replicas. Depending on the implementation, the replication call
 * can fanout or stops here.
 *
 * @opensearch.internal
 */
public abstract class ReplicationProxy<ReplicaRequest> {

    /**
     * Depending on the actual implementation and the passed {@link ReplicationGroup.ReplicationModeAwareShardRouting}, the replication
     * mode is determined using which the replication request is performed on the replica or not.
     *
     */
    public void performOnReplicaProxy(ReplicationProxyRequest replicationProxyRequest, Consumer<ReplicationProxyRequest> originalPerformOnReplica) {
        ReplicationTracker.ReplicationMode replicationMode = determineReplicationMode(replicationProxyRequest.getShardRouting(), replicationProxyRequest.getPrimaryRouting());
        // If the replication modes are 1. Logical replication or 2. Primary term validation, we let the call get performed on the
        // replica shard.
        ///WYOP
        if (replicationMode == ReplicationTracker.ReplicationMode.LOGICAL_REPLICATION || replicationMode == ReplicationTracker.ReplicationMode.PRIMARY_TERM_VALIDATION) {
            originalPerformOnReplica.accept(replicationProxyRequest);
        }
    }

    /**
     * Determines what is the replication mode basis the constructor arguments of the implementation and the current
     * replication mode aware shard routing.
     *
     * @param shardRouting replication mode aware ShardRouting
     * @param primaryRouting primary ShardRouting
     * @return the determined replication mode.
     */
    abstract ReplicationTracker.ReplicationMode determineReplicationMode(
        final ReplicationGroup.ReplicationModeAwareShardRouting shardRouting,
        final ShardRouting primaryRouting
    );

}

/**
 * This implementation of {@link ReplicationProxy} fans out the replication request to current shard routing if
 * it is not the primary and has replication mode as {@link ReplicationTracker.ReplicationMode#LOGICAL_REPLICATION}.
 *
 * @opensearch.internal
 */
private class FanoutReplicationProxy extends ReplicationProxy {

    private FanoutReplicationProxy() {

    }

    @Override
    ReplicationTracker.ReplicationMode determineReplicationMode(ReplicationGroup.ReplicationModeAwareShardRouting shardRouting, ShardRouting primaryRouting) {
        return shardRouting.getShardRouting().isSameAllocation(primaryRouting) == false
            ? ReplicationTracker.ReplicationMode.LOGICAL_REPLICATION
            : ReplicationTracker.ReplicationMode.NONE;
    }
}

/**
 * This implementation of {@link ReplicationProxy} fans out the replication request to current shard routing basis
 * the shard routing's replication mode and replication override policy.
 *
 * @opensearch.internal
 */
private class ReplicationModeAwareOverrideProxy extends ReplicationProxy {

    private final ReplicationOverridePolicy overridePolicy;

    private ReplicationModeAwareOverrideProxy(ReplicationOverridePolicy overridePolicy) {
        assert Objects.nonNull(overridePolicy);
        this.overridePolicy = overridePolicy;
    }

    @Override
    ReplicationTracker.ReplicationMode determineReplicationMode(ReplicationGroup.ReplicationModeAwareShardRouting shardRouting, ShardRouting primaryRouting) {
        ShardRouting currentRouting = shardRouting.getShardRouting();

        // If the current routing is the primary, then it does not need to be replicated
        if (currentRouting.isSameAllocation(primaryRouting)) {
            return ReplicationTracker.ReplicationMode.NONE;
        }

        // If the current routing's replication mode is not NONE, then we return the original replication mode.
        if (shardRouting.getReplicationMode() != ReplicationTracker.ReplicationMode.NONE) {
            return shardRouting.getReplicationMode();
        }

        // If the current routing's replication mode is none, then we check for override and return overridden mode.
        if (Objects.nonNull(overridePolicy)) {
            return overridePolicy.overriddenMode;
        }

        // At the end, return NONE.
        return ReplicationTracker.ReplicationMode.NONE;
    }
}

/**
 * Defines the replication override policy which individual {@link TransportReplicationAction} can implement.
 *
 * @opensearch.internal
 */
public static class ReplicationOverridePolicy {

    private final ReplicationTracker.ReplicationMode overriddenMode;

    public ReplicationOverridePolicy(ReplicationTracker.ReplicationMode overriddenMode) {
        this.overriddenMode = Objects.requireNonNull(overriddenMode);
    }
}
