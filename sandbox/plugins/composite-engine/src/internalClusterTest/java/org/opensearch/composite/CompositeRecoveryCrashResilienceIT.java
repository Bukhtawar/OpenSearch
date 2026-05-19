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

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Tests that highlight the writer generation collision gap in the composite engine.
 *
 * <p>When a node crashes mid-flush during translog replay, VSR rotation can leave
 * orphaned Parquet files on disk at generation N+1. On next recovery, the writer
 * generation counter restarts from the committed catalog snapshot (max gen = N),
 * producing new files at generation N+1 — colliding with the orphan.
 *
 * <p>This test verifies that after recovery, no orphaned (uncommitted) Parquet files
 * exist outside the catalog snapshot, which would indicate a generation collision risk.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeRecoveryCrashResilienceIT extends AbstractCompositeEngineIT {

    private static final String INDEX_NAME = "test-crash-resilience";

    /**
     * After a flush + restart, all Parquet files on disk must be referenced by
     * the committed catalog snapshot. Any unreferenced file is an orphan from a
     * prior crashed flush/replay — evidence that cleanup is missing.
     */
    public void testNoOrphanedParquetFilesAfterRecovery() throws Exception {
        createCompositeIndex(INDEX_NAME);

        // Index enough docs to trigger at least one VSR rotation during indexing
        // (default maxRowsPerVSR = 50000, so this won't trigger rotation,
        //  but flush will write a Parquet file)
        int numDocs = randomIntBetween(50, 200);
        indexDocs(INDEX_NAME, numDocs, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        // Verify catalog snapshot has segments
        long rowsBefore = getRowCount();
        assertTrue("Should have rows after flush", rowsBefore > 0);

        // Get the set of Parquet files referenced by the catalog
        Set<String> catalogReferencedFiles = getCatalogReferencedParquetFiles();
        assertFalse("Catalog should reference at least one Parquet file", catalogReferencedFiles.isEmpty());

        // Restart the node (simulates crash + recovery)
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // After recovery, check for orphaned Parquet files
        Set<String> parquetFilesOnDisk = getParquetFilesOnDisk();
        Set<String> catalogFilesAfterRecovery = getCatalogReferencedParquetFiles();

        // Every Parquet file on disk must be in the catalog snapshot.
        // Any file on disk but NOT in the catalog is an orphan.
        Set<String> orphans = new HashSet<>(parquetFilesOnDisk);
        orphans.removeAll(catalogFilesAfterRecovery);

        assertTrue(
            "Found orphaned Parquet files not referenced by catalog snapshot after recovery: " + orphans
                + ". These could collide with new writer generations on subsequent translog replays.",
            orphans.isEmpty()
        );

        // Verify data integrity — row count should match
        long rowsAfter = getRowCount();
        assertEquals("Row count must survive restart", rowsBefore, rowsAfter);
    }

    /**
     * After indexing, flushing, then indexing MORE docs without flushing, restart.
     * The second batch is in the translog but NOT committed. After recovery:
     * - Translog replay re-indexes the second batch to both formats
     * - The writer generation for the replay-produced files must NOT collide
     *   with any existing file on disk
     */
    public void testWriterGenerationDoesNotCollideAfterCrashMidIndexing() throws Exception {
        createCompositeIndex(INDEX_NAME);

        // Phase 1: index + flush (committed)
        int firstBatch = randomIntBetween(10, 50);
        indexDocs(INDEX_NAME, firstBatch, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        long committedRows = getRowCount();
        assertTrue("Should have committed rows", committedRows > 0);

        // Phase 2: index more (uncommitted — in translog only)
        int secondBatch = randomIntBetween(10, 50);
        indexDocs(INDEX_NAME, secondBatch, firstBatch);
        refreshIndex(INDEX_NAME);
        // NO flush — these ops are in the translog

        // Get max generation from committed catalog (this is what recovery will use)
        long maxGenBeforeCrash = getMaxCommittedGeneration();

        // Simulate crash + recovery (translog will replay the second batch)
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // After recovery, the replayed ops should produce new writer generations
        // that are HIGHER than maxGenBeforeCrash
        long maxGenAfterRecovery = getMaxCommittedGeneration();
        assertTrue(
            "Writer generation after recovery (" + maxGenAfterRecovery + ") must be > "
                + "max committed generation before crash (" + maxGenBeforeCrash + "). "
                + "If equal, translog replay reused a generation that could collide with an orphan.",
            maxGenAfterRecovery > maxGenBeforeCrash
        );

        // Verify all data recovered
        long totalRows = getRowCount();
        assertEquals("All rows (committed + replayed) must survive", firstBatch + secondBatch, totalRows);
    }

    /**
     * Verifies that the writer generation counter is recovered from the catalog
     * snapshot and starts ABOVE any existing file's generation.
     */
    public void testWriterGenerationMonotonicallyIncreases() throws Exception {
        createCompositeIndex(INDEX_NAME);

        // Multiple flush cycles to create multiple generations
        for (int cycle = 0; cycle < 3; cycle++) {
            indexDocs(INDEX_NAME, randomIntBetween(5, 20), cycle * 100);
            refreshIndex(INDEX_NAME);
            flushIndex(INDEX_NAME);
        }

        long maxGenBefore = getMaxCommittedGeneration();
        assertTrue("Should have multiple generations", maxGenBefore >= 3);

        // Restart
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // Index new docs after restart — their generation must be > all prior
        indexDocs(INDEX_NAME, 5, 9000);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        long maxGenAfter = getMaxCommittedGeneration();
        assertTrue(
            "New generation after restart (" + maxGenAfter + ") must be > "
                + "pre-restart max (" + maxGenBefore + ")",
            maxGenAfter > maxGenBefore
        );
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private long getRowCount() throws Exception {
        DataFormatAwareEngine engine = getEngine(INDEX_NAME);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            return ref.get().getSegments()
                .stream()
                .flatMap(seg -> seg.dfGroupedSearchableFiles().values().stream())
                .mapToLong(WriterFileSet::numRows)
                .sum();
        }
    }

    private long getMaxCommittedGeneration() throws Exception {
        DataFormatAwareEngine engine = getEngine(INDEX_NAME);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            return ref.get().getSegments()
                .stream()
                .mapToLong(Segment::generation)
                .max()
                .orElse(0L);
        }
    }

    private Set<String> getCatalogReferencedParquetFiles() throws Exception {
        DataFormatAwareEngine engine = getEngine(INDEX_NAME);
        Set<String> files = new HashSet<>();
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            for (Segment seg : ref.get().getSegments()) {
                for (WriterFileSet wfs : seg.dfGroupedSearchableFiles().values()) {
                    for (String file : wfs.files()) {
                        if (file.endsWith(".parquet") || file.endsWith(".pqt")) {
                            files.add(file);
                        }
                    }
                }
            }
        }
        return files;
    }

    private Set<String> getParquetFilesOnDisk() throws IOException {
        IndexShard shard = getPrimaryShard(INDEX_NAME);
        Path shardPath = shard.shardPath().getDataPath();
        Set<String> parquetFiles = new HashSet<>();
        if (Files.exists(shardPath)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(shardPath, "*.{parquet,pqt}")) {
                for (Path file : stream) {
                    parquetFiles.add(file.getFileName().toString());
                }
            }
        }
        return parquetFiles;
    }
}
