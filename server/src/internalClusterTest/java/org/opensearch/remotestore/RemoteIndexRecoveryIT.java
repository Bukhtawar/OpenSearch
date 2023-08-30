/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.recovery.IndexRecoveryIT;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;

import java.util.Locale;

import static org.opensearch.action.admin.cluster.remotestore.RemoteStoreNode.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.action.admin.cluster.remotestore.RemoteStoreNode.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.action.admin.cluster.remotestore.RemoteStoreNode.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.action.admin.cluster.remotestore.RemoteStoreNode.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteIndexRecoveryIT extends IndexRecoveryIT {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME))
            .put(repositoryNodeAttributes(REPOSITORY_NAME, FsRepository.TYPE, randomRepoPath().toAbsolutePath().toString()))
            .build();
    }

    private Settings repositoryNodeAttributes(String name, String type, String location) {
        String segmentRepoNameAttributeKey = "node.attr." + REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
        String translogRepoNameAttributeKey = "node.attr." + REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;
        String typeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            name
        );
        String settingsAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            name
        );
        return Settings.builder()
            .put(segmentRepoNameAttributeKey, name)
            .put(translogRepoNameAttributeKey, name)
            .put(typeAttributeKey, type)
            .put(settingsAttributeKey + "location", location)
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.REMOTE_STORE, "true")
            .put(FeatureFlags.SEGMENT_REPLICATION_EXPERIMENTAL, "true")
            .build();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s")
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    @After
    public void teardown() {
        assertAcked(clusterAdmin().prepareDeleteRepository(REPOSITORY_NAME));
    }

    @Override
    protected Matcher<Long> getMatcherForThrottling(long value) {
        return Matchers.greaterThanOrEqualTo(value);
    }

    @Override
    protected int numDocs() {
        return randomIntBetween(100, 200);
    }

    @Override
    public void testUsesFileBasedRecoveryIfRetentionLeaseMissing() {
        // Retention lease based tests not applicable for remote store;
    }

    @Override
    public void testPeerRecoveryTrimsLocalTranslog() {
        // Peer recovery usecase not valid for remote enabled indices
    }

    @Override
    public void testHistoryRetention() {
        // History retention not applicable for remote store
    }

    @Override
    public void testUsesFileBasedRecoveryIfOperationsBasedRecoveryWouldBeUnreasonable() {
        // History retention not applicable for remote store
    }

    @Override
    public void testUsesFileBasedRecoveryIfRetentionLeaseAheadOfGlobalCheckpoint() {
        // History retention not applicable for remote store
    }

    @Override
    public void testRecoverLocallyUpToGlobalCheckpoint() {
        // History retention not applicable for remote store
    }

    @Override
    public void testCancelNewShardRecoveryAndUsesExistingShardCopy() {
        // History retention not applicable for remote store
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testReservesBytesDuringPeerRecoveryPhaseOne() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testAllocateEmptyPrimaryResetsGlobalCheckpoint() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testDoesNotCopyOperationsInSafeCommit() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testRepeatedRecovery() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testDisconnectsWhileRecovering() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testTransientErrorsDuringRecoveryAreRetried() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testDoNotInfinitelyWaitForMapping() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testDisconnectsDuringRecovery() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testReplicaRecovery() {

    }
}
