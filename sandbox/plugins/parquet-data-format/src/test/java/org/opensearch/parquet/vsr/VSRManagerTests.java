/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.Version;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.ParquetBaseTests;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.fields.core.data.text.KeywordParquetField;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.writer.MismatchedInputException;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class VSRManagerTests extends ParquetBaseTests {

    private static final DataFormat PARQUET_FORMAT = new ParquetDataFormat();
    private ArrowNativeAllocator nativeAllocator;
    private ArrowBufferPool bufferPool;
    /** Minimal schema VSRManager is constructed with; addDocument tests reconcile metadata fields in via {@link #reconcileMetadata}. */
    private Schema schema;
    private ThreadPool threadPool;
    private IndexSettings indexSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        nativeAllocator = new ArrowNativeAllocator();
        nativeAllocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_INGEST, 0L, Long.MAX_VALUE, null);
        bufferPool = new ArrowBufferPool(Settings.EMPTY, nativeAllocator);
        schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        Settings indexSettingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(indexSettingsBuilder).build();
        indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        Settings settings = Settings.builder().put("node.name", "vsrmanager-test").build();
        threadPool = new ThreadPool(
            settings,
            new FixedExecutorBuilder(
                settings,
                ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME,
                1,
                -1,
                "thread_pool." + ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME
            )
        );
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        bufferPool.close();
        if (nativeAllocator != null) {
            nativeAllocator.close();
            nativeAllocator = null;
        }
        super.tearDown();
    }

    public void testConstructionInitializesActiveVSR() throws Exception {
        String filePath = createTempDir().resolve("init.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);
        assertNotNull(manager.getActiveManagedVSR());
        assertEquals(VSRState.ACTIVE, manager.getActiveManagedVSR().getState());
        // flush handles freeze + close internally
        manager.flush();
    }

    public void testFlushWithNoDataReturnsMetadata() throws Exception {
        String filePath = createTempDir().resolve("empty.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);
        ParquetFileMetadata metadata = manager.flush();
        // With lazy native writer init, flush returns null when no data was written
        assertNull(metadata);
    }

    public void testFlushWithData() throws Exception {
        String filePath = createTempDir().resolve("data.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);

        ManagedVSR active = manager.getActiveManagedVSR();
        IntVector vec = (IntVector) active.getVector("val");
        vec.setSafe(0, 10);
        vec.setSafe(1, 20);
        active.setRowCount(2);

        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(2, metadata.numRows());
        assertNull(manager.getActiveManagedVSR());
    }

    public void testAddDocument() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.addAll(metadataFields());
        fields.add(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null));
        schema = new Schema(fields);

        String filePath = createTempDir().resolve("add-doc.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);

        NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(valField, PARQUET_FORMAT);
        ParquetDocumentInput doc = new ParquetDocumentInput();
        populateMetadataFields(doc);
        doc.addField(valField, 42);
        doc.setRowId("__row_id__", 0);
        manager.addDocument(doc);

        assertEquals(1, manager.getActiveManagedVSR().getRowCount());

        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(1, metadata.numRows());
    }

    public void testMaybeRotateNoOpBelowThreshold() throws Exception {
        String filePath = createTempDir().resolve("norotate.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);
        ManagedVSR original = manager.getActiveManagedVSR();
        original.setRowCount(100);
        manager.maybeRotateActiveVSR();
        assertSame(original, manager.getActiveManagedVSR());
        manager.flush();
    }

    /**
     * Regression for the ingest-pool leak: when {@code close()} fails to drain a background write
     * (awaitPendingWrite throws {@code IOException("Background VSR write failed...")}), it must still
     * release the VSR pool. The pre-fix close() called {@code vsrPool.close()} only after
     * awaitPendingWrite/flush, so a background-write failure skipped it and stranded the per-VSR
     * child allocators' off-heap buffers on the ingest pool for the node's lifetime ("Memory was
     * leaked by query"). Here we buffer data into the active VSR, inject an already-failed
     * pendingWrite so close() takes the throwing path, and assert the pool drains to zero.
     */
    public void testCloseReleasesPoolWhenBackgroundWriteFailed() throws Exception {
        String filePath = createTempDir().resolve("bgwrite-fail.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);

        // Materialize buffers on the active VSR's child allocator so the pool holds bytes.
        ManagedVSR active = manager.getActiveManagedVSR();
        IntVector vec = (IntVector) active.getVector("val");
        for (int i = 0; i < 1000; i++) {
            vec.setSafe(i, i);
        }
        active.setRowCount(1000);
        assertTrue("pool holds buffers before close", bufferPool.getTotalAllocatedBytes() > 0);

        // Inject an already-failed background write so close()'s awaitPendingWrite throws — the exact
        // condition (Background VSR write failed) that used to skip vsrPool.close().
        java.util.concurrent.CompletableFuture<Object> failed = new java.util.concurrent.CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("simulated native write failure"));
        manager.setPendingWrite(failed);

        RuntimeException thrown = expectThrows(RuntimeException.class, manager::close);
        assertTrue(
            "close still surfaces the background-write failure",
            thrown.getMessage() != null && thrown.getMessage().contains("Failed to close VSRManager")
        );

        assertEquals("VSR pool must be released even when the background write failed", 0, bufferPool.getTotalAllocatedBytes());
    }

    public void testMaybeRotateAtThreshold() throws Exception {
        String filePath = createTempDir().resolve("rotate.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);

        ManagedVSR original = manager.getActiveManagedVSR();
        original.setRowCount(50000);
        manager.maybeRotateActiveVSR();

        ManagedVSR newActive = manager.getActiveManagedVSR();
        assertNotSame(original, newActive);
        assertEquals(VSRState.ACTIVE, newActive.getState());
        manager.flush();
    }

    public void testFlushAfterRotation() throws Exception {
        String filePath = createTempDir().resolve("rotate-flush.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);

        // Fill first VSR to trigger rotation
        ManagedVSR first = manager.getActiveManagedVSR();
        IntVector vec1 = (IntVector) first.getVector("val");
        for (int i = 0; i < 50000; i++) {
            vec1.setSafe(i, i);
        }
        first.setRowCount(50000);
        manager.maybeRotateActiveVSR();

        // Add data to second VSR
        ManagedVSR second = manager.getActiveManagedVSR();
        IntVector vec2 = (IntVector) second.getVector("val");
        vec2.setSafe(0, 99);
        second.setRowCount(1);

        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(50001, metadata.numRows());
    }

    public void testRotationAwaitsWhenFrozenSlotOccupied() throws Exception {
        String filePath = createTempDir().resolve("double-rotate.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);

        // Fill first VSR to trigger rotation (async write submitted)
        ManagedVSR first = manager.getActiveManagedVSR();
        IntVector vec1 = (IntVector) first.getVector("val");
        for (int i = 0; i < 100; i++) {
            vec1.setSafe(i, i);
        }
        first.setRowCount(100);
        manager.maybeRotateActiveVSR();

        ManagedVSR second = manager.getActiveManagedVSR();
        assertNotSame(first, second);

        // Fill second VSR — rotation returns false while frozen slot is occupied
        IntVector vec2 = (IntVector) second.getVector("val");
        for (int i = 0; i < 100; i++) {
            vec2.setSafe(i, i + 100);
        }
        second.setRowCount(100);

        // Wait for background write to complete, then rotation should succeed
        Thread.sleep(500);
        manager.maybeRotateActiveVSR();

        ManagedVSR third = manager.getActiveManagedVSR();
        assertNotSame(second, third);
        assertEquals(VSRState.ACTIVE, third.getState());

        manager.flush();
        manager.close();
    }

    public void testRotationWritesHappenOnBackgroundThread() throws Exception {
        String filePath = createTempDir().resolve("bg-thread.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);

        // Fill and rotate
        ManagedVSR first = manager.getActiveManagedVSR();
        IntVector vec = (IntVector) first.getVector("val");
        for (int i = 0; i < 100; i++) {
            vec.setSafe(i, i);
        }
        first.setRowCount(100);
        manager.maybeRotateActiveVSR();

        // New active VSR should be immediately available for writes
        ManagedVSR second = manager.getActiveManagedVSR();
        assertNotNull(second);
        assertEquals(VSRState.ACTIVE, second.getState());
        assertEquals(0, second.getRowCount());

        // Can write to second VSR while background write may still be in progress
        IntVector vec2 = (IntVector) second.getVector("val");
        vec2.setSafe(0, 42);
        second.setRowCount(1);

        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(101, metadata.numRows());
    }

    public void testFlushAwaitsBackgroundWrite() throws Exception {
        String filePath = createTempDir().resolve("flush-await.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);

        // Fill and rotate to trigger background write
        ManagedVSR first = manager.getActiveManagedVSR();
        IntVector vec = (IntVector) first.getVector("val");
        for (int i = 0; i < 100; i++) {
            vec.setSafe(i, i);
        }
        first.setRowCount(100);
        manager.maybeRotateActiveVSR();

        // Add data to second VSR and flush immediately — flush must await background write
        ManagedVSR second = manager.getActiveManagedVSR();
        IntVector vec2 = (IntVector) second.getVector("val");
        vec2.setSafe(0, 999);
        second.setRowCount(1);

        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        // Both the rotated batch (100 rows) and the flushed batch (1 row) should be in the file
        assertEquals(101, metadata.numRows());
    }

    public void testCloseAwaitsBackgroundWrite() throws Exception {
        String filePath = createTempDir().resolve("close-await.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);

        // Fill and rotate to trigger background write
        ManagedVSR first = manager.getActiveManagedVSR();
        IntVector vec = (IntVector) first.getVector("val");
        for (int i = 0; i < 100; i++) {
            vec.setSafe(i, i);
        }
        first.setRowCount(100);
        manager.maybeRotateActiveVSR();

        // Close should not throw — it should await the background write gracefully
        manager.close();
    }

    public void testAddDocumentAfterReconcileSchemaAddsVector() throws Exception {
        String filePath = createTempDir().resolve("unknown-field.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 1L);

        // Simulate a mapping update: the new schema introduces a tag field. reconcileSchema
        // adds the missing vector to the active VSR before addDocument runs.
        Schema updatedSchema = schemaWith("tag", new ArrowType.Utf8());
        manager.reconcileSchema(updatedSchema);

        NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        KeywordFieldMapper.KeywordFieldType tagField = new KeywordFieldMapper.KeywordFieldType("tag");
        assignTestCapabilities(valField, PARQUET_FORMAT);
        assignTestCapabilities(tagField, PARQUET_FORMAT);
        ParquetDocumentInput doc = new ParquetDocumentInput();
        populateMetadataFields(doc);
        doc.setRowId(DocumentInput.ROW_ID_FIELD, 0);
        doc.addField(valField, 42);
        doc.addField(tagField, "hello");
        manager.addDocument(doc);

        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(1, metadata.numRows());
    }

    public void testIsSchemaMutableBeforeAndAfterFlush() throws Exception {
        String filePath = createTempDir().resolve("schema-mutable.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 1L);
        reconcileMetadata(manager);

        assertTrue(manager.isSchemaMutable());

        NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(valField, PARQUET_FORMAT);
        ParquetDocumentInput doc = new ParquetDocumentInput();
        populateMetadataFields(doc);
        doc.setRowId(DocumentInput.ROW_ID_FIELD, 0);
        doc.addField(valField, 1);
        manager.addDocument(doc);

        manager.flush();
        assertFalse(manager.isSchemaMutable());
    }

    public void testSchemaUpdatePropagatesAcrossRotation() throws Exception {
        String filePath = createTempDir().resolve("schema-rotation.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 1, threadPool, 1L);

        NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        KeywordFieldMapper.KeywordFieldType tagField = new KeywordFieldMapper.KeywordFieldType("tag");
        assignTestCapabilities(valField, PARQUET_FORMAT);
        assignTestCapabilities(tagField, PARQUET_FORMAT);

        // Reconcile once before any docs — the tag vector must persist across the VSR
        // rotation triggered by maxRowsPerVSR=1.
        manager.reconcileSchema(schemaWith("tag", new ArrowType.Utf8()));
        {
            ParquetDocumentInput doc1 = new ParquetDocumentInput();
            populateMetadataFields(doc1);
            doc1.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
            doc1.addField(valField, 1);
            doc1.addField(tagField, "a");
            manager.addDocument(doc1);
        }

        {
            ParquetDocumentInput doc2 = new ParquetDocumentInput();
            populateMetadataFields(doc2);
            doc2.setRowId(DocumentInput.ROW_ID_FIELD, 1L);
            doc2.addField(valField, 2);
            doc2.addField(tagField, "b");
            manager.addDocument(doc2); // this would've triggerer the rotation
        }

        {
            ParquetDocumentInput doc3 = new ParquetDocumentInput();
            populateMetadataFields(doc3);
            doc3.setRowId(DocumentInput.ROW_ID_FIELD, 2L);
            doc3.addField(valField, 3);
            doc3.addField(tagField, "c");
            manager.addDocument(doc3); // this would've triggerer the rotation
        }

        ParquetFileMetadata metadata = manager.flush();
        assertEquals(3, metadata.numRows());
    }

    public void testReconcileSchemaAddsMultipleVectorsAtOnce() throws Exception {
        String filePath = createTempDir().resolve("multi-unknown.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 1L);

        // Single reconcileSchema call adds three vectors plus the metadata fields the next
        // addDocument needs.
        List<Field> updatedFields = new ArrayList<>(schema.getFields());
        updatedFields.addAll(metadataFields());
        updatedFields.add(new Field("tag1", FieldType.nullable(new ArrowType.Utf8()), null));
        updatedFields.add(new Field("tag2", FieldType.nullable(new ArrowType.Utf8()), null));
        updatedFields.add(new Field("tag3", FieldType.nullable(new ArrowType.Utf8()), null));
        manager.reconcileSchema(new Schema(updatedFields));

        NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        KeywordFieldMapper.KeywordFieldType tag1Field = new KeywordFieldMapper.KeywordFieldType("tag1");
        KeywordFieldMapper.KeywordFieldType tag2Field = new KeywordFieldMapper.KeywordFieldType("tag2");
        KeywordFieldMapper.KeywordFieldType tag3Field = new KeywordFieldMapper.KeywordFieldType("tag3");
        assignTestCapabilities(valField, PARQUET_FORMAT);
        assignTestCapabilities(tag1Field, PARQUET_FORMAT);
        assignTestCapabilities(tag2Field, PARQUET_FORMAT);
        assignTestCapabilities(tag3Field, PARQUET_FORMAT);

        ParquetDocumentInput doc = new ParquetDocumentInput();
        populateMetadataFields(doc);
        doc.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        doc.addField(valField, 1);
        doc.addField(tag1Field, "a");
        doc.addField(tag2Field, "b");
        doc.addField(tag3Field, "c");
        manager.addDocument(doc);

        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(1, metadata.numRows());
    }

    /**
     * Verifies that {@link VSRManager#getAcceptedRows} is incremented only after a
     * successful row admit, and decremented by {@link VSRManager#rollbackTo(long)}.
     */
    public void testAcceptedRowsCounterTracksAdmitsAndRollbacks() throws Exception {
        String filePath = createTempDir().resolve("accepted-counter.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);
        reconcileMetadata(manager);
        try {
            assertEquals(0L, manager.getAcceptedRows());

            ParquetDocumentInput doc1 = new ParquetDocumentInput();
            populateMetadataFields(doc1);
            doc1.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
            manager.addDocument(doc1);
            assertEquals(1L, manager.getAcceptedRows());

            ParquetDocumentInput doc2 = new ParquetDocumentInput();
            populateMetadataFields(doc2);
            doc2.setRowId(DocumentInput.ROW_ID_FIELD, 1L);
            manager.addDocument(doc2);
            assertEquals(2L, manager.getAcceptedRows());

            manager.rollbackTo(1L);
            assertEquals(1L, manager.getAcceptedRows());

            // Next doc reuses rowId 1 (the slot freed by rollback).
            ParquetDocumentInput doc3 = new ParquetDocumentInput();
            populateMetadataFields(doc3);
            doc3.setRowId(DocumentInput.ROW_ID_FIELD, 1L);
            manager.addDocument(doc3);
            assertEquals(2L, manager.getAcceptedRows());
        } finally {
            manager.close();
        }
    }

    /**
     * Verifies that the rowId column in the active VSR is sorted, contiguous, and
     * starts at 0 — the per-flush invariant that protects cross-format correlation.
     * Reads the rowId vector directly so it doesn't depend on the native flush.
     */
    public void testRowIdColumnIsSortedAndContiguous() throws Exception {
        String filePath = createTempDir().resolve("rowid-monotonic.parquet").toString();
        // Schema must declare __row_id__ for VSRManager to populate the column.
        List<Field> fields = new ArrayList<>(metadataFields());
        fields.add(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null));
        fields.add(new Field(DocumentInput.ROW_ID_FIELD, FieldType.nullable(new ArrowType.Int(64, true)), null));
        Schema schemaWithRowId = new Schema(fields);
        VSRManager manager = new VSRManager(filePath, indexSettings, schemaWithRowId, bufferPool, 50000, threadPool, 0L);
        try {
            for (int i = 0; i < 50; i++) {
                ParquetDocumentInput doc = new ParquetDocumentInput();
                populateMetadataFields(doc);
                doc.setRowId(DocumentInput.ROW_ID_FIELD, (long) i);
                manager.addDocument(doc);
            }

            org.apache.arrow.vector.BigIntVector rowIdVector = (org.apache.arrow.vector.BigIntVector) manager.getActiveManagedVSR()
                .getVector(DocumentInput.ROW_ID_FIELD);
            assertNotNull(rowIdVector);
            for (int i = 0; i < 50; i++) {
                assertEquals("rowId at position " + i + " must equal " + i, (long) i, rowIdVector.get(i));
            }
        } finally {
            manager.close();
        }
    }

    public void testMultiValueFieldWritesListColumn() throws Exception {
        String filePath = createTempDir().resolve("multi-value.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);
        try {
            // Mapping update introduces a keyword field declared multi-valued, so it arrives as a
            // LIST<Utf8> rather than a flat Utf8 column.
            manager.reconcileSchema(schemaWithMultiValue("tags"));

            KeywordFieldMapper.KeywordFieldType tags = new KeywordFieldMapper.KeywordFieldType("tags");
            tags.setMultiValued(true);
            assignTestCapabilities(tags, PARQUET_FORMAT);
            NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
            assignTestCapabilities(valField, PARQUET_FORMAT);

            // Row 0: three values including a duplicate. Row 1: field absent entirely.
            // Row 2: a single value. Covers the three cardinalities in one file.
            ParquetDocumentInput doc0 = new ParquetDocumentInput();
            populateMetadataFields(doc0);
            doc0.setRowId(DocumentInput.ROW_ID_FIELD, 0);
            doc0.addField(valField, 1);
            doc0.addField(tags, "b");
            doc0.addField(tags, "a");
            doc0.addField(tags, "b");
            manager.addDocument(doc0);

            ParquetDocumentInput doc1 = new ParquetDocumentInput();
            populateMetadataFields(doc1);
            doc1.setRowId(DocumentInput.ROW_ID_FIELD, 1);
            doc1.addField(valField, 2);
            manager.addDocument(doc1);

            ParquetDocumentInput doc2 = new ParquetDocumentInput();
            populateMetadataFields(doc2);
            doc2.setRowId(DocumentInput.ROW_ID_FIELD, 2);
            doc2.addField(valField, 3);
            doc2.addField(tags, "solo");
            manager.addDocument(doc2);

            ListVector listVector = (ListVector) manager.getActiveManagedVSR().getVector("tags");
            assertEquals(List.of("b", "a", "b"), listElements(listVector, 0));
            assertTrue("absent field must read back as a null list", listVector.isNull(1));
            assertEquals(List.of("solo"), listElements(listVector, 2));

            ParquetFileMetadata metadata = manager.flush();
            assertNotNull(metadata);
            assertEquals(3, metadata.numRows());
        } finally {
            manager.close();
        }
    }

    /**
     * Adaptive encoding lifecycle: singleton docs accumulate on the scalar fast path, the
     * first genuinely multi-valued doc promotes the column in place carrying prior rows as
     * single-element lists, and later rows write through the list path.
     */
    public void testAdaptivePromotionOnFirstArrayCarriesPriorRows() throws Exception {
        String filePath = createTempDir().resolve("adaptive-promote.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);
        try {
            manager.reconcileSchema(schemaWithMultiValue("tags"));
            KeywordFieldMapper.KeywordFieldType tags = new KeywordFieldMapper.KeywordFieldType("tags");
            tags.setMultiValued(true);
            assignTestCapabilities(tags, PARQUET_FORMAT);
            NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
            assignTestCapabilities(valField, PARQUET_FORMAT);

            // Row 0: singleton — scalar fast path. Row 1: field absent — null. Both pre-promotion.
            ParquetDocumentInput doc0 = new ParquetDocumentInput();
            populateMetadataFields(doc0);
            doc0.setRowId(DocumentInput.ROW_ID_FIELD, 0);
            doc0.addField(valField, 1);
            doc0.addField(tags, "first");
            manager.addDocument(doc0);

            ParquetDocumentInput doc1 = new ParquetDocumentInput();
            populateMetadataFields(doc1);
            doc1.setRowId(DocumentInput.ROW_ID_FIELD, 1);
            doc1.addField(valField, 2);
            manager.addDocument(doc1);

            assertTrue("column must still be the optimistic scalar", manager.getActiveManagedVSR().isPromotable("tags"));
            assertTrue(manager.getActiveManagedVSR().getVector("tags") instanceof VarCharVector);

            // Row 2: the first real array — triggers in-place promotion.
            ParquetDocumentInput doc2 = new ParquetDocumentInput();
            populateMetadataFields(doc2);
            doc2.setRowId(DocumentInput.ROW_ID_FIELD, 2);
            doc2.addField(valField, 3);
            doc2.addField(tags, "x");
            doc2.addField(tags, "y");
            manager.addDocument(doc2);

            assertFalse("column must have been promoted", manager.getActiveManagedVSR().isPromotable("tags"));
            ListVector listVector = (ListVector) manager.getActiveManagedVSR().getVector("tags");
            assertEquals("pre-promotion singleton must carry over as a one-element list", List.of("first"), listElements(listVector, 0));
            assertTrue("pre-promotion absent row must carry over as a null list", listVector.isNull(1));
            assertEquals(List.of("x", "y"), listElements(listVector, 2));

            // Row 3: singleton after promotion — written through the list path.
            ParquetDocumentInput doc3 = new ParquetDocumentInput();
            populateMetadataFields(doc3);
            doc3.setRowId(DocumentInput.ROW_ID_FIELD, 3);
            doc3.addField(valField, 4);
            doc3.addField(tags, "last");
            manager.addDocument(doc3);
            assertEquals(List.of("last"), listElements(listVector, 3));

            ParquetFileMetadata metadata = manager.flush();
            assertNotNull(metadata);
            assertEquals(4, metadata.numRows());
        } finally {
            manager.close();
        }
    }

    /**
     * Freeze-time canonicalization: a declared-LIST column that only ever saw singletons is
     * promoted wholesale when the VSR freezes, so the exported batch always matches the
     * declared LIST schema — the flush-time encoding decision, pinned to LIST.
     */
    public void testAdaptiveColumnCanonicalizedToListAtFreeze() throws Exception {
        String filePath = createTempDir().resolve("adaptive-freeze.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);
        try {
            manager.reconcileSchema(schemaWithMultiValue("tags"));
            KeywordFieldMapper.KeywordFieldType tags = new KeywordFieldMapper.KeywordFieldType("tags");
            tags.setMultiValued(true);
            assignTestCapabilities(tags, PARQUET_FORMAT);
            NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
            assignTestCapabilities(valField, PARQUET_FORMAT);

            for (int i = 0; i < 3; i++) {
                ParquetDocumentInput doc = new ParquetDocumentInput();
                populateMetadataFields(doc);
                doc.setRowId(DocumentInput.ROW_ID_FIELD, i);
                doc.addField(valField, i);
                doc.addField(tags, "v" + i);
                manager.addDocument(doc);
            }
            ManagedVSR active = manager.getActiveManagedVSR();
            assertTrue("all-singleton column must still be scalar before freeze", active.isPromotable("tags"));

            // Freeze directly (bypassing flush) to pin where canonicalization happens.
            active.moveToFrozen();
            Field tagsField = active.getSchema().getFields().stream().filter(f -> f.getName().equals("tags")).findFirst().orElseThrow();
            assertEquals("frozen batch must expose the declared LIST shape", ArrowType.List.INSTANCE, tagsField.getType());
            assertEquals(ParquetField.LIST_ELEMENT_NAME, tagsField.getChildren().get(0).getName());
        } finally {
            manager.close();
        }
    }

    /**
     * A document that triggers promotion mid-write and then fails on a later field leaves the
     * promotion in place (content-preserving, so it must not be undone) with the failed row
     * partially written. The row was never admitted (rowCount unchanged), so the next document
     * reuses the same row index and must be able to rewrite it through the list path — this
     * exercises Arrow's ListVector offset/lastSet rewind for an index written twice.
     */
    public void testFailedDocAfterPromotionLeavesVSRWritable() throws Exception {
        String filePath = createTempDir().resolve("promotion-failed-doc.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);
        try {
            manager.reconcileSchema(schemaWithMultiValue("tags"));
            KeywordFieldMapper.KeywordFieldType tags = new KeywordFieldMapper.KeywordFieldType("tags");
            tags.setMultiValued(true);
            assignTestCapabilities(tags, PARQUET_FORMAT);
            NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
            assignTestCapabilities(valField, PARQUET_FORMAT);
            // A field with Parquet capabilities but no vector in the VSR: addDocument writes
            // earlier pairs, then throws MismatchedInputException on this one.
            KeywordFieldMapper.KeywordFieldType unknown = new KeywordFieldMapper.KeywordFieldType("unknown_field");
            assignTestCapabilities(unknown, PARQUET_FORMAT);

            // Row 0: clean singleton — scalar fast path.
            ParquetDocumentInput doc0 = new ParquetDocumentInput();
            populateMetadataFields(doc0);
            doc0.setRowId(DocumentInput.ROW_ID_FIELD, 0);
            doc0.addField(valField, 1);
            doc0.addField(tags, "keep");
            manager.addDocument(doc0);

            // Failing doc at row 1: the array promotes the column, then the unknown field throws.
            ParquetDocumentInput badDoc = new ParquetDocumentInput();
            populateMetadataFields(badDoc);
            badDoc.setRowId(DocumentInput.ROW_ID_FIELD, 1);
            badDoc.addField(tags, "doomed1");
            badDoc.addField(tags, "doomed2");
            badDoc.addField(unknown, "boom");
            expectThrows(MismatchedInputException.class, () -> manager.addDocument(badDoc));

            // Promotion must persist (it is content-preserving) and the pre-existing row intact.
            assertFalse("promotion must not be rolled back", manager.getActiveManagedVSR().isPromotable("tags"));
            ListVector listVector = (ListVector) manager.getActiveManagedVSR().getVector("tags");
            assertEquals(List.of("keep"), listElements(listVector, 0));
            // The failed row was never admitted.
            assertEquals(1, manager.getAcceptedRows());
            assertEquals(1, manager.getActiveManagedVSR().getRowCount());

            // The caller-driven rollback for a rejected doc is a no-op at the admitted count.
            manager.rollbackTo(1);

            // Row 1 (same index the failed doc partially wrote) must be rewritable — both the
            // list column (offset/lastSet rewind) and via a subsequent array.
            ParquetDocumentInput doc1 = new ParquetDocumentInput();
            populateMetadataFields(doc1);
            doc1.setRowId(DocumentInput.ROW_ID_FIELD, 1);
            doc1.addField(valField, 2);
            doc1.addField(tags, "replay-a");
            doc1.addField(tags, "replay-b");
            doc1.addField(tags, "replay-c");
            manager.addDocument(doc1);

            ParquetDocumentInput doc2 = new ParquetDocumentInput();
            populateMetadataFields(doc2);
            doc2.setRowId(DocumentInput.ROW_ID_FIELD, 2);
            doc2.addField(valField, 3);
            doc2.addField(tags, "tail");
            manager.addDocument(doc2);

            assertEquals(List.of("keep"), listElements(listVector, 0));
            assertEquals("failed row must be fully overwritten by the readmitted doc", List.of("replay-a", "replay-b", "replay-c"), listElements(listVector, 1));
            assertEquals(List.of("tail"), listElements(listVector, 2));

            ParquetFileMetadata metadata = manager.flush();
            assertNotNull(metadata);
            assertEquals(3, metadata.numRows());
        } finally {
            manager.close();
        }
    }

    /**
     * Same failed-doc-after-promotion setup, but the promotion is triggered by a LATER doc than
     * the failed one: the failed doc writes a singleton through the scalar fast path, is
     * rejected on its unknown field, and the next doc's array promotes — the stale scalar slot
     * from the never-admitted row must be carried into the list as the readmitted row's value
     * only if rewritten, and the promotion row accounting must use admitted rows only.
     */
    public void testFailedScalarWriteThenPromotionUsesAdmittedRowsOnly() throws Exception {
        String filePath = createTempDir().resolve("promotion-after-failed-scalar.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);
        try {
            manager.reconcileSchema(schemaWithMultiValue("tags"));
            KeywordFieldMapper.KeywordFieldType tags = new KeywordFieldMapper.KeywordFieldType("tags");
            tags.setMultiValued(true);
            assignTestCapabilities(tags, PARQUET_FORMAT);
            NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
            assignTestCapabilities(valField, PARQUET_FORMAT);
            KeywordFieldMapper.KeywordFieldType unknown = new KeywordFieldMapper.KeywordFieldType("unknown_field");
            assignTestCapabilities(unknown, PARQUET_FORMAT);

            // Failing doc at row 0: singleton scalar write, then rejection.
            ParquetDocumentInput badDoc = new ParquetDocumentInput();
            populateMetadataFields(badDoc);
            badDoc.setRowId(DocumentInput.ROW_ID_FIELD, 0);
            badDoc.addField(tags, "stale");
            badDoc.addField(unknown, "boom");
            expectThrows(MismatchedInputException.class, () -> manager.addDocument(badDoc));
            assertEquals(0, manager.getAcceptedRows());
            assertTrue("no array seen yet — column must still be scalar", manager.getActiveManagedVSR().isPromotable("tags"));

            // Row 0 rewritten by a doc whose array triggers promotion at rowCount=0: the
            // promotion must carry ZERO admitted rows (the stale scalar slot is unreachable
            // through admitted accounting) and the array lands as row 0's value.
            ParquetDocumentInput doc0 = new ParquetDocumentInput();
            populateMetadataFields(doc0);
            doc0.setRowId(DocumentInput.ROW_ID_FIELD, 0);
            doc0.addField(valField, 1);
            doc0.addField(tags, "real-a");
            doc0.addField(tags, "real-b");
            manager.addDocument(doc0);

            ListVector listVector = (ListVector) manager.getActiveManagedVSR().getVector("tags");
            assertEquals(List.of("real-a", "real-b"), listElements(listVector, 0));
            assertEquals(1, manager.flush().numRows());
        } finally {
            manager.close();
        }
    }

    /**
     * A declared-LIST column added by a late mapping update that no document ever writes must
     * survive freeze-time canonicalization: its scalar seed has under-allocated buffers, and
     * the promoter must not crash scanning validity — every row exports as a null list.
     */
    public void testNeverWrittenAdaptiveColumnSurvivesFreeze() throws Exception {
        String filePath = createTempDir().resolve("never-written-freeze.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);
        try {
            reconcileMetadata(manager);
            NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
            assignTestCapabilities(valField, PARQUET_FORMAT);

            // Two rows admitted BEFORE the LIST column exists.
            for (int i = 0; i < 2; i++) {
                ParquetDocumentInput doc = new ParquetDocumentInput();
                populateMetadataFields(doc);
                doc.setRowId(DocumentInput.ROW_ID_FIELD, i);
                doc.addField(valField, i);
                manager.addDocument(doc);
            }
            // Late mapping update introduces the LIST column; no document ever writes it.
            manager.reconcileSchema(schemaWithMultiValue("tags"));
            assertTrue(manager.getActiveManagedVSR().isPromotable("tags"));

            // Flush freezes and canonicalizes the never-written scalar seed.
            ParquetFileMetadata metadata = manager.flush();
            assertNotNull(metadata);
            assertEquals(2, metadata.numRows());
        } finally {
            manager.close();
        }
    }

    public void testMultiValueFieldWritesEmptyListDistinctFromAbsent() throws Exception {
        String filePath = createTempDir().resolve("multi-value-empty.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);
        try {
            manager.reconcileSchema(schemaWithMultiValue("tags"));
            NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
            assignTestCapabilities(valField, PARQUET_FORMAT);

            // An explicit "tags": [] parses to zero addField calls, so the writer never sees the
            // field and the row is null — same as absent. Documented here so the distinction
            // between [] and absent is a deliberate, tested choice rather than an accident.
            ParquetDocumentInput doc = new ParquetDocumentInput();
            populateMetadataFields(doc);
            doc.setRowId(DocumentInput.ROW_ID_FIELD, 0);
            doc.addField(valField, 1);
            manager.addDocument(doc);

            // Adaptive encoding: a column no document ever multi-valued is still an optimistic
            // scalar accumulator here; the row reads back null either way, and freeze-time
            // canonicalization (inside flush) exports it as a null list.
            assertTrue(manager.getActiveManagedVSR().isPromotable("tags"));
            assertTrue(manager.getActiveManagedVSR().getVector("tags").isNull(0));
            assertEquals(1, manager.flush().numRows());
        } finally {
            manager.close();
        }
    }

    public void testReconcileSchemaPreservesListChildren() throws Exception {
        String filePath = createTempDir().resolve("reconcile-children.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);
        try {
            assertTrue(manager.reconcileSchema(schemaWithMultiValue("tags")));

            // Adaptive encoding: the declared LIST column is seeded as an optimistic scalar of
            // its element type, so the physical vector is a VarCharVector until promotion.
            assertTrue(manager.getActiveManagedVSR().isPromotable("tags"));
            assertTrue(
                "adaptive seed must be the element-typed scalar, got "
                    + manager.getActiveManagedVSR().getVector("tags").getClass().getSimpleName(),
                manager.getActiveManagedVSR().getVector("tags") instanceof VarCharVector
            );

            // The DECLARED schema must keep the LIST shape with its element child intact:
            // reconcileSchema propagates it to the pool, so a dropped child here would leave
            // rotated VSRs (and the exported file schema) without an element vector.
            Field declaredTags = manager.getActiveManagedVSR()
                .getDeclaredSchema()
                .getFields()
                .stream()
                .filter(f -> f.getName().equals("tags"))
                .findFirst()
                .orElseThrow();
            assertEquals(ArrowType.List.INSTANCE, declaredTags.getType());
            assertEquals(1, declaredTags.getChildren().size());
            assertEquals(ParquetField.LIST_ELEMENT_NAME, declaredTags.getChildren().get(0).getName());
            assertEquals(new ArrowType.Utf8(), declaredTags.getChildren().get(0).getType());

            // After promotion the physical schema must carry the same declared shape: the
            // schema — which is what exportSchema hands to the native writer, and therefore
            // what determines the Parquet leaf path "tags.list.element" — keeps the declared
            // element name even though Arrow renames the vector's own child to "$data$".
            ListVector listVector = (ListVector) manager.getActiveManagedVSR().promoteToList("tags");
            assertTrue(
                "list vector must have a typed element vector, got " + listVector.getDataVector().getClass().getSimpleName(),
                listVector.getDataVector() instanceof VarCharVector
            );
            Field tagsField = manager.getActiveManagedVSR()
                .getSchema()
                .getFields()
                .stream()
                .filter(f -> f.getName().equals("tags"))
                .findFirst()
                .orElseThrow();
            assertEquals(ArrowType.List.INSTANCE, tagsField.getType());
            assertEquals(1, tagsField.getChildren().size());
            assertEquals(ParquetField.LIST_ELEMENT_NAME, tagsField.getChildren().get(0).getName());
            assertEquals(new ArrowType.Utf8(), tagsField.getChildren().get(0).getType());
        } finally {
            manager.close();
        }
    }

    /** Reads back the elements of one row of a list vector. */
    private static List<String> listElements(ListVector listVector, int row) {
        int start = listVector.getOffsetBuffer().getInt((long) row * 4);
        int end = listVector.getOffsetBuffer().getInt((long) (row + 1) * 4);
        VarCharVector data = (VarCharVector) listVector.getDataVector();
        List<String> values = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            values.add(new String(data.get(i), StandardCharsets.UTF_8));
        }
        return values;
    }

    /** Test schema plus metadata fields plus a keyword field declared multi-valued. */
    private Schema schemaWithMultiValue(String name) {
        List<Field> fields = new ArrayList<>(schema.getFields());
        fields.addAll(metadataFields());
        fields.add(new KeywordParquetField().toArrowField(name, true));
        return new Schema(fields);
    }

    /** Returns a copy of the test schema with one extra field appended (alongside metadata fields). */
    private Schema schemaWith(String name, ArrowType type) {
        List<Field> fields = new ArrayList<>(schema.getFields());
        fields.addAll(metadataFields());
        fields.add(new Field(name, FieldType.nullable(type), null));
        return new Schema(fields);
    }

    /**
     * Simulates the production mapping-update path: reconcile the active VSR with the
     * production-shaped schema (val + metadata fields) so that subsequent
     * {@code addDocument} calls find every vector they need.
     */
    private void reconcileMetadata(VSRManager manager) {
        List<Field> fields = new ArrayList<>(schema.getFields());
        fields.addAll(metadataFields());
        manager.reconcileSchema(new Schema(fields));
    }

    public void testAddDocumentAfterSuccessfulBackgroundWriteDoesNotThrow() throws Exception {
        // Use a very low rotation threshold to trigger background writes frequently
        List<Field> fields = new ArrayList<>();
        fields.addAll(metadataFields());
        fields.add(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null));
        schema = new Schema(fields);

        String filePath = createTempDir().resolve("bg-write-success.parquet").toString();
        int lowThreshold = randomIntBetween(2, 5);
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, lowThreshold, threadPool, 0L);

        NumberFieldMapper.NumberFieldType valField = createNumberField("val", NumberFieldMapper.NumberType.INTEGER);

        // Run multiple rotation cycles — each cycle fills the VSR to threshold,
        // triggers background write, waits for completion, then verifies next addDocument works
        int cycles = randomIntBetween(3, 8);
        int rowId = 0;
        for (int cycle = 0; cycle < cycles; cycle++) {
            for (int i = 0; i < lowThreshold; i++) {
                ParquetDocumentInput doc = new ParquetDocumentInput();
                populateMetadataFields(doc);
                doc.addField(valField, rowId);
                doc.setRowId(DocumentInput.ROW_ID_FIELD, rowId);
                manager.addDocument(doc);
                rowId++;
            }

            // Wait for background write to complete using assertBusy
            assertBusy(() -> {
                Future<?> f = manager.getPendingWrite();
                assertTrue("Background write should complete", f == null || f.isDone());
            });

            // This addDocument must NOT throw — verifies the fix for the
            // exceptionNow() bug on successfully completed futures
            ParquetDocumentInput nextDoc = new ParquetDocumentInput();
            populateMetadataFields(nextDoc);
            nextDoc.addField(valField, rowId);
            nextDoc.setRowId(DocumentInput.ROW_ID_FIELD, rowId);
            manager.addDocument(nextDoc);
            rowId++;
        }

        manager.flush();
    }

    public void testContinuousAddDocumentAcrossMultipleRotationsWithoutWaiting() throws Exception {
        // Continuously add documents across many rotations without ever waiting for
        // background writes — verifies no error when future is still running or not yet done
        List<Field> fields = new ArrayList<>();
        fields.addAll(metadataFields());
        fields.add(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null));
        schema = new Schema(fields);

        String filePath = createTempDir().resolve("continuous-add.parquet").toString();
        int lowThreshold = randomIntBetween(2, 4);
        int totalDocs = lowThreshold * randomIntBetween(5, 12);
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, lowThreshold, threadPool, 0L);

        NumberFieldMapper.NumberFieldType valField = createNumberField("val", NumberFieldMapper.NumberType.INTEGER);

        // Add all docs in a tight loop — no waiting between rotations
        for (int i = 0; i < totalDocs; i++) {
            ParquetDocumentInput doc = new ParquetDocumentInput();
            populateMetadataFields(doc);
            doc.addField(valField, i);
            doc.setRowId(DocumentInput.ROW_ID_FIELD, i);
            manager.addDocument(doc);
        }

        // Flush at the end — must succeed regardless of pending write state
        ParquetFileMetadata metadata = manager.flush();
        assertNotNull(metadata);
        assertEquals(totalDocs, metadata.numRows());
    }

    public void testAllowsDistinctFieldsInSingleDocument() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.addAll(metadataFields());
        fields.add(new Field("price", FieldType.nullable(new ArrowType.Int(32, true)), null));
        fields.add(new Field("qty", FieldType.nullable(new ArrowType.Int(32, true)), null));
        schema = new Schema(fields);

        String filePath = createTempDir().resolve("distinct.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 50000, threadPool, 0L);

        NumberFieldMapper.NumberFieldType priceField = new NumberFieldMapper.NumberFieldType("price", NumberFieldMapper.NumberType.INTEGER);
        NumberFieldMapper.NumberFieldType qtyField = new NumberFieldMapper.NumberFieldType("qty", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(priceField, PARQUET_FORMAT);
        assignTestCapabilities(qtyField, PARQUET_FORMAT);

        ParquetDocumentInput doc = new ParquetDocumentInput();
        populateMetadataFields(doc);
        doc.addField(priceField, 10);
        doc.addField(qtyField, 5);
        doc.setRowId(DocumentInput.ROW_ID_FIELD, 0);

        manager.addDocument(doc);
        assertEquals(1, manager.getActiveManagedVSR().getRowCount());
        manager.flush();
    }

    /**
     * Correctness gap 1: rollback of the very document whose array triggered mid-batch
     * promotion. The promotion must survive (un-promoting is unsafe), the rolled-back row's
     * list state must be fully rewound, and — the ghost case — a following document that
     * leaves the field ABSENT must read back a null list, not the rejected document's values.
     */
    public void testRollbackOfPromotionTriggeringDocLeavesNoGhost() throws Exception {
        String filePath = createTempDir().resolve("rollback-promotion.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);
        try {
            manager.reconcileSchema(schemaWithMultiValue("tags"));
            KeywordFieldMapper.KeywordFieldType tags = new KeywordFieldMapper.KeywordFieldType("tags");
            tags.setMultiValued(true);
            assignTestCapabilities(tags, PARQUET_FORMAT);

            // Row 0: singleton on the scalar fast path.
            ParquetDocumentInput doc0 = new ParquetDocumentInput();
            populateMetadataFields(doc0);
            doc0.setRowId(DocumentInput.ROW_ID_FIELD, 0);
            doc0.addField(tags, "a");
            manager.addDocument(doc0);
            assertTrue(manager.getActiveManagedVSR().isPromotable("tags"));

            // Row 1: the promotion-triggering array — accepted by parquet, then rejected by
            // the secondary format: CompositeWriter rolls the composite back to 1 row.
            ParquetDocumentInput doc1 = new ParquetDocumentInput();
            populateMetadataFields(doc1);
            doc1.setRowId(DocumentInput.ROW_ID_FIELD, 1);
            doc1.addField(tags, "ghost1");
            doc1.addField(tags, "ghost2");
            manager.addDocument(doc1);
            assertFalse("array doc must have promoted the column", manager.getActiveManagedVSR().isPromotable("tags"));
            manager.rollbackTo(1L);

            // Promotion survives rollback: the column stays LIST, holding row 0 as ["a"].
            ListVector listVector = (ListVector) manager.getActiveManagedVSR().getVector("tags");
            assertEquals(List.of("a"), listElements(listVector, 0));

            // Row 1 reused by a doc WITHOUT the field: must be a null list, not the ghost.
            ParquetDocumentInput doc2 = new ParquetDocumentInput();
            populateMetadataFields(doc2);
            doc2.setRowId(DocumentInput.ROW_ID_FIELD, 1);
            manager.addDocument(doc2);
            assertTrue("rolled-back slot reused by an absent field must be null", listVector.isNull(1));

            // Row 2: writes resume through the list path, offsets self-consistent.
            ParquetDocumentInput doc3 = new ParquetDocumentInput();
            populateMetadataFields(doc3);
            doc3.setRowId(DocumentInput.ROW_ID_FIELD, 2);
            doc3.addField(tags, "p");
            doc3.addField(tags, "q");
            manager.addDocument(doc3);

            assertEquals(List.of("a"), listElements(listVector, 0));
            assertTrue(listVector.isNull(1));
            assertEquals(List.of("p", "q"), listElements(listVector, 2));

            ParquetFileMetadata metadata = manager.flush();
            assertNotNull(metadata);
            assertEquals(3, metadata.numRows());
        } finally {
            manager.close();
        }
    }

    /**
     * Correctness gap 1 (pre-promotion variant): a singleton document written on the scalar
     * fast path is rolled back while the column is still the optimistic scalar. The trimmed
     * slot must read as null, and freeze-time canonicalization must carry it over as a null
     * list — not resurrect the rejected value.
     */
    public void testRollbackOnScalarPathThenFreezeCarriesNoGhost() throws Exception {
        String filePath = createTempDir().resolve("rollback-scalar.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);
        try {
            manager.reconcileSchema(schemaWithMultiValue("tags"));
            KeywordFieldMapper.KeywordFieldType tags = new KeywordFieldMapper.KeywordFieldType("tags");
            tags.setMultiValued(true);
            assignTestCapabilities(tags, PARQUET_FORMAT);

            ParquetDocumentInput doc0 = new ParquetDocumentInput();
            populateMetadataFields(doc0);
            doc0.setRowId(DocumentInput.ROW_ID_FIELD, 0);
            doc0.addField(tags, "keep");
            manager.addDocument(doc0);

            ParquetDocumentInput doc1 = new ParquetDocumentInput();
            populateMetadataFields(doc1);
            doc1.setRowId(DocumentInput.ROW_ID_FIELD, 1);
            doc1.addField(tags, "ghost");
            manager.addDocument(doc1);
            manager.rollbackTo(1L);
            assertTrue("column must still be the optimistic scalar", manager.getActiveManagedVSR().isPromotable("tags"));

            // Row 1 reused by an absent-field doc, then freeze-time canonicalization.
            ParquetDocumentInput doc2 = new ParquetDocumentInput();
            populateMetadataFields(doc2);
            doc2.setRowId(DocumentInput.ROW_ID_FIELD, 1);
            manager.addDocument(doc2);

            ParquetFileMetadata metadata = manager.flush();
            assertNotNull(metadata);
            assertEquals(2, metadata.numRows());
        } finally {
            manager.close();
        }
    }

    /**
     * Correctness gap 2: a LIST column added by mapping update (reconcileSchema) while the
     * active VSR already holds rows. The new column must seed as an adaptive scalar, prior
     * rows must carry over as null lists on promotion, and the promotion-triggering array
     * must land intact.
     */
    public void testReconcileAddsListColumnMidBatchThenPromotes() throws Exception {
        String filePath = createTempDir().resolve("reconcile-midbatch.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);
        try {
            reconcileMetadata(manager);
            NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
            assignTestCapabilities(valField, PARQUET_FORMAT);

            // Row 0 exists before the mapping update introduces `tags`.
            ParquetDocumentInput doc0 = new ParquetDocumentInput();
            populateMetadataFields(doc0);
            doc0.setRowId(DocumentInput.ROW_ID_FIELD, 0);
            doc0.addField(valField, 1);
            manager.addDocument(doc0);

            // Mapping update mid-batch: declared LIST column joins an active VSR with 1 row.
            manager.reconcileSchema(schemaWithMultiValue("tags"));
            assertTrue("reconciled LIST column must seed as adaptive scalar", manager.getActiveManagedVSR().isPromotable("tags"));

            KeywordFieldMapper.KeywordFieldType tags = new KeywordFieldMapper.KeywordFieldType("tags");
            tags.setMultiValued(true);
            assignTestCapabilities(tags, PARQUET_FORMAT);

            // Row 1: array — promotes with one pre-existing row to carry over.
            ParquetDocumentInput doc1 = new ParquetDocumentInput();
            populateMetadataFields(doc1);
            doc1.setRowId(DocumentInput.ROW_ID_FIELD, 1);
            doc1.addField(valField, 2);
            doc1.addField(tags, "m");
            doc1.addField(tags, "n");
            manager.addDocument(doc1);

            ListVector listVector = (ListVector) manager.getActiveManagedVSR().getVector("tags");
            assertTrue("row predating the mapping update must be a null list", listVector.isNull(0));
            assertEquals(List.of("m", "n"), listElements(listVector, 1));

            ParquetFileMetadata metadata = manager.flush();
            assertNotNull(metadata);
            assertEquals(2, metadata.numRows());
        } finally {
            manager.close();
        }
    }

    /**
     * Correctness gap 3: bulk with a rejected document adjacent to the promotion boundary.
     * The rollback must not disturb the accepted array that triggered promotion (before it)
     * nor the arrays written after writes resume — the neighbor-corruption invariant from
     * PR #22685's bulk tests, extended to the adaptive encoding.
     */
    public void testBulkNeighborsSurviveRollbackAtPromotionBoundary() throws Exception {
        String filePath = createTempDir().resolve("bulk-neighbors.parquet").toString();
        VSRManager manager = new VSRManager(filePath, indexSettings, schema, bufferPool, 100, threadPool, 0L);
        try {
            manager.reconcileSchema(schemaWithMultiValue("tags"));
            KeywordFieldMapper.KeywordFieldType tags = new KeywordFieldMapper.KeywordFieldType("tags");
            tags.setMultiValued(true);
            assignTestCapabilities(tags, PARQUET_FORMAT);

            // Row 0: singleton. Row 1: promotion-triggering array (ACCEPTED — stays).
            ParquetDocumentInput doc0 = new ParquetDocumentInput();
            populateMetadataFields(doc0);
            doc0.setRowId(DocumentInput.ROW_ID_FIELD, 0);
            doc0.addField(tags, "a");
            manager.addDocument(doc0);

            ParquetDocumentInput doc1 = new ParquetDocumentInput();
            populateMetadataFields(doc1);
            doc1.setRowId(DocumentInput.ROW_ID_FIELD, 1);
            doc1.addField(tags, "x");
            doc1.addField(tags, "y");
            manager.addDocument(doc1);

            // Row 2: written, then rejected by the secondary format — rolled back.
            ParquetDocumentInput doc2 = new ParquetDocumentInput();
            populateMetadataFields(doc2);
            doc2.setRowId(DocumentInput.ROW_ID_FIELD, 2);
            doc2.addField(tags, "bad1");
            doc2.addField(tags, "bad2");
            doc2.addField(tags, "bad3");
            manager.addDocument(doc2);
            manager.rollbackTo(2L);

            // Rows 2-3: writes resume; slot 2 is reused.
            ParquetDocumentInput doc3 = new ParquetDocumentInput();
            populateMetadataFields(doc3);
            doc3.setRowId(DocumentInput.ROW_ID_FIELD, 2);
            doc3.addField(tags, "z");
            manager.addDocument(doc3);

            ParquetDocumentInput doc4 = new ParquetDocumentInput();
            populateMetadataFields(doc4);
            doc4.setRowId(DocumentInput.ROW_ID_FIELD, 3);
            doc4.addField(tags, "b");
            manager.addDocument(doc4);

            ListVector listVector = (ListVector) manager.getActiveManagedVSR().getVector("tags");
            assertEquals("neighbor before the rollback must be intact", List.of("a"), listElements(listVector, 0));
            assertEquals("the accepted promotion-trigger must be intact", List.of("x", "y"), listElements(listVector, 1));
            assertEquals("the reused slot must hold the resumed doc, not the rejected one", List.of("z"), listElements(listVector, 2));
            assertEquals(List.of("b"), listElements(listVector, 3));

            ParquetFileMetadata metadata = manager.flush();
            assertNotNull(metadata);
            assertEquals(4, metadata.numRows());
        } finally {
            manager.close();
        }
    }

}
