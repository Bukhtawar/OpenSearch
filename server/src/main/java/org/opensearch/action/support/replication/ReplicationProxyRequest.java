/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.cluster.routing.ShardRouting;

public class ReplicationProxyRequest<ReplicaRequest> {

    private final ShardRouting shardRouting;

    private final ShardRouting primaryRouting;

    private final long globalCheckpoint;

    private final long maxSeqNoOfUpdatesOrDeletes;

    private final PendingReplicationActions pendingReplicationActions;

    private final ReplicaRequest replicaRequest;

    public ReplicationProxyRequest(ShardRouting shardRouting, ShardRouting primaryRouting, long globalCheckpoint, long maxSeqNoOfUpdatesOrDeletes,
                                   PendingReplicationActions pendingReplicationActions, ReplicaRequest replicaRequest) {
        this.shardRouting = shardRouting;
        this.primaryRouting = primaryRouting;
        this.globalCheckpoint = globalCheckpoint;
        this.maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletes;
        this.pendingReplicationActions = pendingReplicationActions;
        this.replicaRequest = replicaRequest;
    }

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    public ShardRouting getPrimaryRouting() {
        return primaryRouting;
    }

    public long getGlobalCheckpoint() {
        return globalCheckpoint;
    }

    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return maxSeqNoOfUpdatesOrDeletes;
    }

    public PendingReplicationActions getPendingReplicationActions() {
        return pendingReplicationActions;
    }

    public ReplicaRequest getReplicaRequest() {
        return replicaRequest;
    }
}
