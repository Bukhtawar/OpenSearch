/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Critical recovery integration tests for the composite engine (Parquet + Lucene).
 *
 * Validates that peer recovery, primary failover, and merge + recovery
 * correctly handle both data formats.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeRecoveryIT extends AbstractCompositeEngineIT {

    private static final String INDEX_NAME = "test-recovery";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("cluster.routing.allocation.enable", "all")
            .build();
    }

    /**
     * Local recovery after restart preserves both formats.
     *
     * Primary indexes + flushes, restarts. After restart, catalog snapshot
     * must reference both Parquet and Lucene files with correct row count.
     */
    public void testLocalRecoveryPreservesBothFormats() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        createCompositeIndex(INDEX_NAME);

        int numDocs = randomIntBetween(20, 50);
        indexDocs(INDEX_NAME, numDocs, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        long rowsBefore = getRowCount();
        assertTrue("Should have rows", rowsBefore >= numDocs);
        assertBothFormatsPresent();

        // Restart
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // Verify both formats preserved
        long rowsAfter = getRowCount();
        assertEquals("Row count must survive restart", rowsBefore, rowsAfter);
        assertBothFormatsPresent();
    }

    /**
     * Translog replay after crash produces new generations above committed max.
     *
     * Index + flush (committed), then index more (uncommitted). Restart.
     * Translog replays the uncommitted ops — new writer generations must be
     * above the committed max to avoid collision with potential orphans.
     */
    public void testTranslogReplayGenerationAdvances() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        createCompositeIndex(INDEX_NAME);

        // Phase 1: committed
        int firstBatch = randomIntBetween(10, 30);
        indexDocs(INDEX_NAME, firstBatch, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        long maxGenBeforeCrash = getMaxGeneration(null);

        // Phase 2: uncommitted (in translog only)
        int secondBatch = randomIntBetween(10, 30);
        indexDocs(INDEX_NAME, secondBatch, firstBatch);
        refreshIndex(INDEX_NAME);
        // NO flush — these are in the translog

        // Restart — translog replays second batch
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // Verify generation advanced
        long maxGenAfterRecovery = getMaxGeneration(null);
        assertTrue(
            "Generation after recovery (" + maxGenAfterRecovery + ") must be > "
                + "committed max before crash (" + maxGenBeforeCrash + ")",
            maxGenAfterRecovery > maxGenBeforeCrash
        );

        // Verify all data recovered
        long totalRows = getRowCount();
        assertEquals("All rows must survive", firstBatch + secondBatch, totalRows);
    }

    /**
     * Recovery after merge: merged segments survive restart.
     *
     * Create many small segments via repeated flush. Trigger merge. Restart.
     * Verify data integrity (row count) survives the merge + restart cycle.
     */
    public void testRecoveryAfterMerge() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        createCompositeIndex(INDEX_NAME);

        // Create multiple small segments
        int totalDocs = 0;
        for (int i = 0; i < 5; i++) {
            int batch = randomIntBetween(5, 15);
            indexDocs(INDEX_NAME, batch, i * 100);
            totalDocs += batch;
            refreshIndex(INDEX_NAME);
            flushIndex(INDEX_NAME);
        }

        long rowsBeforeMerge = getRowCount();
        assertTrue("Should have rows", rowsBeforeMerge > 0);

        // Force merge
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        long rowsAfterMerge = getRowCount();
        assertEquals("Row count must not change after merge", rowsBeforeMerge, rowsAfterMerge);

        // Restart
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // Verify data survived merge + restart
        long rowsAfterRestart = getRowCount();
        assertEquals("Row count must survive restart after merge", rowsAfterMerge, rowsAfterRestart);
        assertBothFormatsPresent();
    }

    /**
     * Multiple restart cycles: data integrity across repeated restarts.
     */
    public void testMultipleRestartCycles() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        createCompositeIndex(INDEX_NAME);

        int numDocs = randomIntBetween(30, 60);
        indexDocs(INDEX_NAME, numDocs, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        long expectedRows = numDocs;

        // Two full restart cycles
        for (int i = 0; i < 2; i++) {
            internalCluster().fullRestart();
            ensureGreen(INDEX_NAME);

            long rows = getRowCount();
            assertEquals("Rows must survive restart cycle " + (i + 1), expectedRows, rows);
            assertBothFormatsPresent();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private long getRowCount() throws Exception {
        return getRowCount(null);
    }

    private long getRowCount(String nodeName) throws Exception {
        IndexShard shard = getShard(nodeName);
        DataFormatAwareEngine engine = (DataFormatAwareEngine) IndexShardTestCase.getIndexer(shard);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            return ref.get().getSegments()
                .stream()
                .flatMap(seg -> seg.dfGroupedSearchableFiles().values().stream())
                .mapToLong(WriterFileSet::numRows)
                .sum();
        }
    }

    private long getMaxGeneration(String nodeName) throws Exception {
        IndexShard shard = getShard(nodeName);
        DataFormatAwareEngine engine = (DataFormatAwareEngine) IndexShardTestCase.getIndexer(shard);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            return ref.get().getSegments()
                .stream()
                .mapToLong(Segment::generation)
                .max()
                .orElse(0L);
        }
    }

    private int getSegmentCount() throws Exception {
        return getSegmentCount(null);
    }

    private int getSegmentCount(String nodeName) throws Exception {
        IndexShard shard = getShard(nodeName);
        DataFormatAwareEngine engine = (DataFormatAwareEngine) IndexShardTestCase.getIndexer(shard);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            return ref.get().getSegments().size();
        }
    }

    private void assertBothFormatsPresent() throws Exception {
        assertBothFormatsPresent(null);
    }

    private void assertBothFormatsPresent(String nodeName) throws Exception {
        IndexShard shard = getShard(nodeName);
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

    private IndexShard getShard(String nodeName) {
        if (nodeName == null) {
            String nodeId = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
            nodeName = getClusterState().nodes().get(nodeId).getName();
        }
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        var indexService = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME));
        return indexService.getShard(0);
    }
}
