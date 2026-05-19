/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for the composite engine (Parquet + Lucene) with remote store enabled.
 *
 * Validates that recovery from remote store correctly handles both data formats
 * and exposes the translog replay duplication bug in the remote store context.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeRemoteStoreRecoveryIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "composite-remote-recovery";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(
                ParquetDataFormatPlugin.class,
                CompositeDataFormatPlugin.class,
                LucenePlugin.class,
                DataFusionPlugin.class
            )
        ).collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    private Settings compositeRemoteIndexSettings() {
        return Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
    }

    private void createIndex() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX_NAME)
                .setSettings(compositeRemoteIndexSettings())
                .setMapping("name", "type=keyword", "value", "type=integer")
        );
        ensureGreen(INDEX_NAME);
    }

    private void indexDocs(int count, int startId) {
        for (int i = startId; i < startId + count; i++) {
            assertEquals(
                RestStatus.CREATED,
                client().prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource("name", "doc_" + i, "value", i).get().status()
            );
        }
    }

    /**
     * Validates both formats survive a full restart with remote store.
     * Flush commits both formats, remote store uploads both via DataFormatAwareRemoteDirectory.
     * After restart, remote store downloads both and engine opens correctly.
     */
    public void testRemoteStoreRecoveryPreservesBothFormats() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createIndex();

        int numDocs = randomIntBetween(20, 50);
        indexDocs(numDocs, 0);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();

        long rowsBefore = getRowCount();
        assertTrue("Should have rows before restart", rowsBefore > 0);
        assertBothFormatsPresent();

        // Full restart — recovery from remote store
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        long rowsAfter = getRowCount();
        assertEquals("Row count must survive remote store recovery", rowsBefore, rowsAfter);
        assertBothFormatsPresent();
    }

    /**
     * Exposes the translog replay duplication bug with remote store.
     *
     * Index docs, flush (commits to remote), then index more (uncommitted, in translog).
     * Restart — remote store downloads committed files + translog.
     * Translog replay re-indexes the uncommitted ops. If the committed ops are also
     * replayed (because translog wasn't properly trimmed), rows double.
     */
    public void testTranslogReplayDoesNotDuplicateRowsWithRemoteStore() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createIndex();

        // Phase 1: committed
        int firstBatch = randomIntBetween(10, 30);
        indexDocs(firstBatch, 0);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();

        // Phase 2: uncommitted (in translog, uploaded to remote translog)
        int secondBatch = randomIntBetween(5, 15);
        indexDocs(secondBatch, firstBatch);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        // NO flush — in translog only

        long expectedTotal = firstBatch + secondBatch;

        // Restart — downloads from remote store + replays translog
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        long actualRows = getRowCount();
        assertEquals(
            "Translog replay must NOT duplicate rows already committed to Parquet. "
                + "Expected " + expectedTotal + " but got " + actualRows + ". "
                + "If actual > expected, translog replayed ops that were already in committed Parquet files.",
            expectedTotal,
            actualRows
        );
    }

    /**
     * Validates generation advances correctly after remote store recovery.
     */
    public void testGenerationAdvancesAfterRemoteStoreRecovery() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createIndex();

        int numDocs = randomIntBetween(10, 30);
        indexDocs(numDocs, 0);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();

        long maxGenBefore = getMaxGeneration();

        // Restart
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // Index new docs — must get higher generation
        indexDocs(5, 1000);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();

        long maxGenAfter = getMaxGeneration();
        assertTrue(
            "Generation after remote recovery + new writes (" + maxGenAfter + ") must be > pre-restart max (" + maxGenBefore + ")",
            maxGenAfter > maxGenBefore
        );
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private long getRowCount() throws Exception {
        IndexShard shard = getPrimaryShard();
        DataFormatAwareEngine engine = (DataFormatAwareEngine) IndexShardTestCase.getIndexer(shard);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            return ref.get().getSegments()
                .stream()
                .flatMap(seg -> seg.dfGroupedSearchableFiles().values().stream())
                .mapToLong(WriterFileSet::numRows)
                .sum();
        }
    }

    private long getMaxGeneration() throws Exception {
        IndexShard shard = getPrimaryShard();
        DataFormatAwareEngine engine = (DataFormatAwareEngine) IndexShardTestCase.getIndexer(shard);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            return ref.get().getSegments()
                .stream()
                .mapToLong(Segment::generation)
                .max()
                .orElse(0L);
        }
    }

    private void assertBothFormatsPresent() throws Exception {
        IndexShard shard = getPrimaryShard();
        DataFormatAwareEngine engine = (DataFormatAwareEngine) IndexShardTestCase.getIndexer(shard);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            for (Segment seg : ref.get().getSegments()) {
                assertTrue(
                    "Segment gen=" + seg.generation() + " must have parquet files",
                    seg.dfGroupedSearchableFiles().containsKey("parquet")
                );
                assertTrue(
                    "Segment gen=" + seg.generation() + " must have lucene files",
                    seg.dfGroupedSearchableFiles().containsKey("lucene")
                );
            }
        }
    }

    private IndexShard getPrimaryShard() {
        String nodeId = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        String nodeName = getClusterState().nodes().get(nodeId).getName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        var indexService = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME));
        return indexService.getShard(0);
    }
}
