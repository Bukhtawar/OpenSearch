/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.store.CompositeStoreDirectory;
import org.opensearch.index.store.RemoteIndexOutput;
import org.opensearch.index.store.UploadedSegmentMetadata;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompositeRemoteDirectoryTests extends OpenSearchTestCase {

    private static final Logger logger = LogManager.getLogger(CompositeRemoteDirectoryTests.class);

    private BlobStore blobStore;
    private BlobPath baseBlobPath;
    private CompositeRemoteDirectory compositeRemoteDirectory;
    private BlobContainer defaultBlobContainer;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        blobStore = mock(BlobStore.class);
        baseBlobPath = new BlobPath().add("base");
        defaultBlobContainer = mock(BlobContainer.class);

        when(blobStore.blobContainer(baseBlobPath)).thenReturn(defaultBlobContainer);

        compositeRemoteDirectory = new CompositeRemoteDirectory(
            blobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new ConcurrentHashMap<>(),
            logger
        );
    }

    // ═══════════════════════════════════════════════════════════════
    // getBlobContainerForFormat - Lazy creation
    // ═══════════════════════════════════════════════════════════════

    public void testGetBlobContainerForFormat_createsLazily() {
        BlobContainer luceneContainer = mock(BlobContainer.class);
        BlobPath lucenePath = baseBlobPath.add("lucene");
        when(blobStore.blobContainer(lucenePath)).thenReturn(luceneContainer);

        BlobContainer result1 = compositeRemoteDirectory.getBlobContainerForFormat("lucene");
        assertSame(luceneContainer, result1);

        // Second call should return the same cached container
        BlobContainer result2 = compositeRemoteDirectory.getBlobContainerForFormat("lucene");
        assertSame(result1, result2);

        // blobStore.blobContainer should only be called once for "lucene" format
        verify(blobStore).blobContainer(lucenePath);
    }

    public void testGetBlobContainerForFormat_multipleFormats() {
        BlobContainer luceneContainer = mock(BlobContainer.class);
        BlobContainer parquetContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(baseBlobPath.add("lucene"))).thenReturn(luceneContainer);
        when(blobStore.blobContainer(baseBlobPath.add("parquet"))).thenReturn(parquetContainer);

        BlobContainer lucResult = compositeRemoteDirectory.getBlobContainerForFormat("lucene");
        BlobContainer pqResult = compositeRemoteDirectory.getBlobContainerForFormat("parquet");

        assertSame(luceneContainer, lucResult);
        assertSame(parquetContainer, pqResult);
        assertNotSame(lucResult, pqResult);
    }

    // ═══════════════════════════════════════════════════════════════
    // copyFrom - async multi-stream
    // ═══════════════════════════════════════════════════════════════

    public void testCopyFrom_asyncMultiStreamContainer_returnsTrue() throws Exception {
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(blobStore.blobContainer(baseBlobPath.add("lucene"))).thenReturn(asyncContainer);
        when(asyncContainer.path()).thenReturn(baseBlobPath.add("lucene"));

        // Pre-register the format so the internal map has the container
        compositeRemoteDirectory.getBlobContainerForFormat("lucene");

        CompositeStoreDirectory storeDirectory = mock(CompositeStoreDirectory.class);
        FileMetadata fileMetadata = new FileMetadata("lucene", "_0.si");
        IndexInput mockInput = mock(IndexInput.class);
        when(storeDirectory.openInput(fileMetadata, IOContext.READONCE)).thenReturn(mockInput);
        when(mockInput.length()).thenReturn(100L);

        ActionListener<Void> listener = mock(ActionListener.class);

        boolean result = compositeRemoteDirectory.copyFrom(
            storeDirectory, fileMetadata, "_0.si__uuid", IOContext.DEFAULT,
            () -> {}, listener, false
        );

        assertTrue("Should return true when container is AsyncMultiStreamBlobContainer", result);
    }

    public void testCopyFrom_nonAsyncContainer_returnsFalse() {
        BlobContainer regularContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(baseBlobPath.add("parquet"))).thenReturn(regularContainer);

        compositeRemoteDirectory.getBlobContainerForFormat("parquet");

        CompositeStoreDirectory storeDirectory = mock(CompositeStoreDirectory.class);
        FileMetadata fileMetadata = new FileMetadata("parquet", "data.parquet");
        ActionListener<Void> listener = mock(ActionListener.class);

        boolean result = compositeRemoteDirectory.copyFrom(
            storeDirectory, fileMetadata, "data.parquet__uuid", IOContext.DEFAULT,
            () -> {}, listener, false
        );

        assertFalse("Should return false when container is not AsyncMultiStreamBlobContainer", result);
    }

    // ═══════════════════════════════════════════════════════════════
    // fileLength
    // ═══════════════════════════════════════════════════════════════

    public void testFileLength_found() throws IOException {
        BlobContainer container = mock(BlobContainer.class);
        when(blobStore.blobContainer(baseBlobPath.add("lucene"))).thenReturn(container);

        BlobMetadata blobMetadata = new PlainBlobMetadata("_0.si", 1024);
        when(container.listBlobsByPrefixInSortedOrder("_0.si", 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC))
            .thenReturn(List.of(blobMetadata));

        compositeRemoteDirectory.getBlobContainerForFormat("lucene");

        FileMetadata fm = new FileMetadata("lucene", "_0.si");
        long length = compositeRemoteDirectory.fileLength(fm);
        assertEquals(1024L, length);
    }

    public void testFileLength_containerNull_throws() {
        // Don't register any container for "nonexistent" format
        FileMetadata fm = new FileMetadata("nonexistent", "file.dat");
        expectThrows(NoSuchFileException.class, () -> compositeRemoteDirectory.fileLength(fm));
    }

    public void testFileLength_fileNotFound_throws() throws IOException {
        BlobContainer container = mock(BlobContainer.class);
        when(blobStore.blobContainer(baseBlobPath.add("lucene"))).thenReturn(container);
        when(container.listBlobsByPrefixInSortedOrder("_missing.si", 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC))
            .thenReturn(Collections.emptyList());

        compositeRemoteDirectory.getBlobContainerForFormat("lucene");

        FileMetadata fm = new FileMetadata("lucene", "_missing.si");
        expectThrows(NoSuchFileException.class, () -> compositeRemoteDirectory.fileLength(fm));
    }

    // ═══════════════════════════════════════════════════════════════
    // createOutput
    // ═══════════════════════════════════════════════════════════════

    public void testCreateOutput_containerExists() throws IOException {
        BlobContainer container = mock(BlobContainer.class);
        when(blobStore.blobContainer(baseBlobPath.add("parquet"))).thenReturn(container);

        compositeRemoteDirectory.getBlobContainerForFormat("parquet");

        RemoteIndexOutput output = compositeRemoteDirectory.createOutput("remote_file.parquet", "parquet", IOContext.DEFAULT);
        assertNotNull(output);
    }

    public void testCreateOutput_containerMissing_throws() {
        // Don't register format "unknown"
        expectThrows(IOException.class, () ->
            compositeRemoteDirectory.createOutput("remote_file.dat", "unknown", IOContext.DEFAULT));
    }

    public void testCreateOutput_nullFilename_throws() {
        expectThrows(IllegalArgumentException.class, () ->
            compositeRemoteDirectory.createOutput(null, "lucene", IOContext.DEFAULT));
    }

    public void testCreateOutput_emptyFilename_throws() {
        expectThrows(IllegalArgumentException.class, () ->
            compositeRemoteDirectory.createOutput("", "lucene", IOContext.DEFAULT));
    }

    // ═══════════════════════════════════════════════════════════════
    // openInput
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInput_success() throws IOException {
        BlobContainer container = mock(BlobContainer.class);
        when(blobStore.blobContainer(baseBlobPath.add("lucene"))).thenReturn(container);

        byte[] content = "test content".getBytes();
        InputStream stream = new ByteArrayInputStream(content);
        when(container.readBlob("_0.si__uuid")).thenReturn(stream);

        compositeRemoteDirectory.getBlobContainerForFormat("lucene");

        FileMetadata fm = new FileMetadata("lucene", "_0.si__uuid");
        IndexInput input = compositeRemoteDirectory.openInput(fm, content.length, IOContext.DEFAULT);
        assertNotNull(input);
        input.close();
    }

    public void testOpenInput_containerNull_throws() {
        FileMetadata fm = new FileMetadata("nonexistent", "file.dat");
        expectThrows(IOException.class, () -> compositeRemoteDirectory.openInput(fm, 100, IOContext.DEFAULT));
    }

    public void testOpenInput_nullFile_throws() {
        FileMetadata fm = new FileMetadata("lucene", null);
        expectThrows(IllegalArgumentException.class, () ->
            compositeRemoteDirectory.openInput(fm, 100, IOContext.DEFAULT));
    }

    public void testOpenInput_emptyFile_throws() {
        FileMetadata fm = new FileMetadata("lucene", "");
        expectThrows(IllegalArgumentException.class, () ->
            compositeRemoteDirectory.openInput(fm, 100, IOContext.DEFAULT));
    }

    // ═══════════════════════════════════════════════════════════════
    // deleteFile
    // ═══════════════════════════════════════════════════════════════

    public void testDeleteFile_delegatesToBlobContainer() throws IOException {
        BlobContainer container = mock(BlobContainer.class);
        when(blobStore.blobContainer(baseBlobPath.add("lucene"))).thenReturn(container);

        compositeRemoteDirectory.getBlobContainerForFormat("lucene");

        UploadedSegmentMetadata metadata = new UploadedSegmentMetadata("_0.si", "_0.si__uuid", "12345", 1024, "lucene");
        compositeRemoteDirectory.deleteFile(metadata);

        verify(container).deleteBlobsIgnoringIfNotExists(Collections.singletonList("_0.si__uuid"));
    }

    // ═══════════════════════════════════════════════════════════════
    // delete / close
    // ═══════════════════════════════════════════════════════════════

    public void testDelete_clearsAllContainers() throws IOException {
        BlobContainer luceneContainer = mock(BlobContainer.class);
        BlobContainer parquetContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(baseBlobPath.add("lucene"))).thenReturn(luceneContainer);
        when(blobStore.blobContainer(baseBlobPath.add("parquet"))).thenReturn(parquetContainer);

        compositeRemoteDirectory.getBlobContainerForFormat("lucene");
        compositeRemoteDirectory.getBlobContainerForFormat("parquet");

        compositeRemoteDirectory.delete();

        verify(luceneContainer).delete();
        verify(parquetContainer).delete();
    }

    public void testClose_clearsContainerMap() throws IOException {
        BlobContainer container = mock(BlobContainer.class);
        when(blobStore.blobContainer(baseBlobPath.add("lucene"))).thenReturn(container);

        compositeRemoteDirectory.getBlobContainerForFormat("lucene");

        compositeRemoteDirectory.close();

        // After close, requesting the same format should create a new container
        BlobContainer newContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(baseBlobPath.add("lucene"))).thenReturn(newContainer);

        BlobContainer result = compositeRemoteDirectory.getBlobContainerForFormat("lucene");
        assertSame(newContainer, result);
    }

    // ═══════════════════════════════════════════════════════════════
    // toString
    // ═══════════════════════════════════════════════════════════════

    public void testToString() {
        String str = compositeRemoteDirectory.toString();
        assertTrue(str.contains("CompositeRemoteDirectory"));
        assertTrue(str.contains("basePath"));
    }

    // ═══════════════════════════════════════════════════════════════
    // copyFrom - sync (non-async path)
    // ═══════════════════════════════════════════════════════════════

    public void testCopyFrom_sync() throws IOException {
        BlobContainer container = mock(BlobContainer.class);
        when(blobStore.blobContainer(baseBlobPath.add("lucene"))).thenReturn(container);

        compositeRemoteDirectory.getBlobContainerForFormat("lucene");

        CompositeStoreDirectory storeDirectory = mock(CompositeStoreDirectory.class);
        FileMetadata src = new FileMetadata("lucene", "_0.si");
        IndexInput mockInput = mock(IndexInput.class);
        when(storeDirectory.openInput(src, IOContext.READONCE)).thenReturn(mockInput);
        when(mockInput.length()).thenReturn(0L);

        RemoteIndexOutput mockOutput = mock(RemoteIndexOutput.class);
        // The sync copyFrom reads from storeDirectory.openInput and writes to createOutput
        // We verify the method doesn't throw
        compositeRemoteDirectory.copyFrom(storeDirectory, src, "_0.si__uuid", IOContext.DEFAULT);

        verify(storeDirectory).openInput(src, IOContext.READONCE);
    }

    // ═══════════════════════════════════════════════════════════════
    // pendingDownloadMergedSegments
    // ═══════════════════════════════════════════════════════════════

    public void testPendingDownloadMergedSegments_passedCorrectly() {
        Map<FileMetadata, String> pendingSegments = new ConcurrentHashMap<>();
        FileMetadata fm = new FileMetadata("lucene", "_1.si");
        pendingSegments.put(fm, "_1.si__uuid");

        CompositeRemoteDirectory dir = new CompositeRemoteDirectory(
            blobStore, baseBlobPath,
            UnaryOperator.identity(), UnaryOperator.identity(),
            UnaryOperator.identity(), UnaryOperator.identity(),
            pendingSegments, logger
        );

        assertTrue(dir.pendingDownloadMergedSegments.containsKey(fm));
        assertEquals("_1.si__uuid", dir.pendingDownloadMergedSegments.get(fm));
    }
}
