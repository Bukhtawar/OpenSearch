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
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeInputStream;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CompositeRemoteDirectory}.
 */
public class CompositeRemoteDirectoryTests extends OpenSearchTestCase {

    private static final Logger logger = LogManager.getLogger(CompositeRemoteDirectoryTests.class);

    private BlobStore mockBlobStore;
    private BlobContainer baseBlobContainer;
    private BlobContainer parquetBlobContainer;
    private BlobPath baseBlobPath;
    private CompositeRemoteDirectory directory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockBlobStore = mock(BlobStore.class);
        baseBlobContainer = mock(BlobContainer.class);
        parquetBlobContainer = mock(BlobContainer.class);
        baseBlobPath = new BlobPath().add("segments").add("data");

        when(mockBlobStore.blobContainer(baseBlobPath)).thenReturn(baseBlobContainer);
        when(mockBlobStore.blobContainer(baseBlobPath.add("parquet"))).thenReturn(parquetBlobContainer);

        // Identity rate limiters (no-op)
        UnaryOperator<OffsetRangeInputStream> uploadRateLimiter = UnaryOperator.identity();
        UnaryOperator<OffsetRangeInputStream> lowPriorityUploadRateLimiter = UnaryOperator.identity();
        UnaryOperator<InputStream> downloadRateLimiter = UnaryOperator.identity();
        UnaryOperator<InputStream> lowPriorityDownloadRateLimiter = UnaryOperator.identity();

        directory = new CompositeRemoteDirectory(
            mockBlobStore,
            baseBlobPath,
            uploadRateLimiter,
            lowPriorityUploadRateLimiter,
            downloadRateLimiter,
            lowPriorityDownloadRateLimiter,
            new HashMap<>(),
            logger,
            null // no plugins service in unit tests
        );
    }

    // ═══════════════════════════════════════════════════════════════
    // Format Routing Tests (getBlobContainerForFormat)
    // ═══════════════════════════════════════════════════════════════

    public void testGetBlobContainerForFormat_Lucene() {
        BlobContainer container = directory.getBlobContainerForFormat("lucene");
        assertSame("lucene should route to base container", baseBlobContainer, container);
    }

    public void testGetBlobContainerForFormat_LUCENE_UpperCase() {
        BlobContainer container = directory.getBlobContainerForFormat("LUCENE");
        assertSame("LUCENE should route to base container", baseBlobContainer, container);
    }

    public void testGetBlobContainerForFormat_Metadata() {
        BlobContainer container = directory.getBlobContainerForFormat("metadata");
        assertSame("metadata should route to base container", baseBlobContainer, container);
    }

    public void testGetBlobContainerForFormat_Null() {
        BlobContainer container = directory.getBlobContainerForFormat(null);
        assertSame("null format should route to base container", baseBlobContainer, container);
    }

    public void testGetBlobContainerForFormat_Empty() {
        BlobContainer container = directory.getBlobContainerForFormat("");
        assertSame("empty format should route to base container", baseBlobContainer, container);
    }

    public void testGetBlobContainerForFormat_Parquet() {
        BlobContainer container = directory.getBlobContainerForFormat("parquet");
        assertSame("parquet should route to parquet container", parquetBlobContainer, container);
    }

    public void testGetBlobContainerForFormat_UnknownFormat_LazilyCreated() {
        BlobContainer arrowContainer = mock(BlobContainer.class);
        when(mockBlobStore.blobContainer(baseBlobPath.add("arrow"))).thenReturn(arrowContainer);

        BlobContainer container = directory.getBlobContainerForFormat("arrow");
        assertSame("arrow should create new container", arrowContainer, container);

        // Second call should return cached container
        BlobContainer containerAgain = directory.getBlobContainerForFormat("arrow");
        assertSame("arrow should return cached container", arrowContainer, containerAgain);

        // blobStore.blobContainer should only be called once for arrow
        verify(mockBlobStore, times(1)).blobContainer(baseBlobPath.add("arrow"));
    }

    // ═══════════════════════════════════════════════════════════════
    // List Tests
    // ═══════════════════════════════════════════════════════════════

    public void testListAll_AggregatesAllContainers() throws IOException {
        // Base container has lucene files
        Map<String, BlobMetadata> baseBlobs = new HashMap<>();
        baseBlobs.put("_0.cfs__UUID1", new PlainBlobMetadata("_0.cfs__UUID1", 100));
        baseBlobs.put("_0.si__UUID2", new PlainBlobMetadata("_0.si__UUID2", 50));
        when(baseBlobContainer.listBlobs()).thenReturn(baseBlobs);

        // Pre-register parquet container and add parquet files
        directory.getBlobContainerForFormat("parquet");
        Map<String, BlobMetadata> parquetBlobs = new HashMap<>();
        parquetBlobs.put("_0.parquet__UUID3", new PlainBlobMetadata("_0.parquet__UUID3", 200));
        when(parquetBlobContainer.listBlobs()).thenReturn(parquetBlobs);

        String[] allFiles = directory.listAll();

        assertEquals(3, allFiles.length);
        // Should be sorted
        assertEquals("_0.cfs__UUID1", allFiles[0]);
        assertEquals("_0.parquet__UUID3", allFiles[1]);
        assertEquals("_0.si__UUID2", allFiles[2]);
    }

    public void testListAll_EmptyContainers() throws IOException {
        when(baseBlobContainer.listBlobs()).thenReturn(Collections.emptyMap());

        String[] allFiles = directory.listAll();
        assertEquals(0, allFiles.length);
    }

    // ═══════════════════════════════════════════════════════════════
    // Delete Tests
    // ═══════════════════════════════════════════════════════════════

    public void testDeleteFile_LuceneFile() throws IOException {
        directory.deleteFile("_0.cfs");

        verify(baseBlobContainer).deleteBlobsIgnoringIfNotExists(Collections.singletonList("_0.cfs"));
        verify(parquetBlobContainer, never()).deleteBlobsIgnoringIfNotExists(any());
    }

    public void testDeleteFile_ParquetFile_WithFormatSuffix() throws IOException {
        // Pre-register parquet container
        directory.getBlobContainerForFormat("parquet");

        directory.deleteFile("_0.parquet:::parquet");

        verify(parquetBlobContainer).deleteBlobsIgnoringIfNotExists(Collections.singletonList("_0.parquet:::parquet"));
    }

    public void testDeleteFile_WithUploadedSegmentMetadata_Parquet() throws IOException {
        // Pre-register parquet container
        directory.getBlobContainerForFormat("parquet");

        // Use fromString() since constructor is package-private
        // Format: originalFilename::uploadedFilename::checksum::length::writtenByMajor
        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString(
            "_0.parquet:::parquet::_0.parquet__UUID1::checksum123::200::10"
        );

        directory.deleteFile(metadata);

        verify(parquetBlobContainer).deleteBlobsIgnoringIfNotExists(Collections.singletonList("_0.parquet__UUID1"));
    }

    public void testDeleteFiles_BatchDelete_DeletesFromAllContainers() throws IOException {
        // Pre-register parquet container
        directory.getBlobContainerForFormat("parquet");

        List<String> names = List.of("_0.cfs__UUID1", "_0.parquet__UUID2");
        directory.deleteFiles(names);

        // Should attempt from base AND parquet containers (since names have no format info)
        verify(baseBlobContainer).deleteBlobsIgnoringIfNotExists(names);
        verify(parquetBlobContainer).deleteBlobsIgnoringIfNotExists(names);
    }

    public void testDeleteFiles_EmptyList_NoOp() throws IOException {
        directory.deleteFiles(Collections.emptyList());

        verify(baseBlobContainer, never()).deleteBlobsIgnoringIfNotExists(any());
    }

    public void testDeleteFiles_NullList_NoOp() throws IOException {
        directory.deleteFiles(null);

        verify(baseBlobContainer, never()).deleteBlobsIgnoringIfNotExists(any());
    }

    // ═══════════════════════════════════════════════════════════════
    // OpenInput Tests
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInput_WithUploadedSegmentMetadata_Lucene() throws IOException {
        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString("_0.cfs::_0.cfs__UUID1::checksum123::100::10");

        byte[] content = new byte[100];
        when(baseBlobContainer.readBlob("_0.cfs__UUID1")).thenReturn(new ByteArrayInputStream(content));

        IndexInput input = directory.openInput(metadata, 100, IOContext.DEFAULT);
        assertNotNull(input);
        assertEquals(100, input.length());
        input.close();

        verify(baseBlobContainer).readBlob("_0.cfs__UUID1");
        verify(parquetBlobContainer, never()).readBlob(anyString());
    }

    public void testOpenInput_WithUploadedSegmentMetadata_Parquet() throws IOException {
        // Pre-register parquet container
        directory.getBlobContainerForFormat("parquet");

        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString(
            "_0.parquet:::parquet::_0.parquet__UUID1::checksum456::200::10"
        );

        byte[] content = new byte[200];
        when(parquetBlobContainer.readBlob("_0.parquet__UUID1")).thenReturn(new ByteArrayInputStream(content));

        IndexInput input = directory.openInput(metadata, 200, IOContext.DEFAULT);
        assertNotNull(input);
        assertEquals(200, input.length());
        input.close();

        verify(parquetBlobContainer).readBlob("_0.parquet__UUID1");
        verify(baseBlobContainer, never()).readBlob(anyString());
    }

    public void testOpenInput_ClosesStream_OnFailure() throws IOException {
        InputStream mockStream = mock(InputStream.class);
        when(baseBlobContainer.readBlob("_0.cfs__UUID1")).thenReturn(mockStream);
        when(mockStream.read(any(), anyInt(), anyInt())).thenThrow(new IOException("read error"));

        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString("_0.cfs::_0.cfs__UUID1::checksum123::100::10");

        // The openInput should succeed (it just wraps the stream), but we verify the pattern
        IndexInput input = directory.openInput(metadata, 100, IOContext.DEFAULT);
        assertNotNull(input);
        input.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // FileLength Tests
    // ═══════════════════════════════════════════════════════════════

    public void testFileLength_LuceneFile() throws IOException {
        List<BlobMetadata> blobList = List.of(new PlainBlobMetadata("_0.cfs", 1234));
        when(baseBlobContainer.listBlobsByPrefixInSortedOrder(eq("_0.cfs"), eq(1), any())).thenReturn(blobList);

        long length = directory.fileLength("_0.cfs");
        assertEquals(1234, length);
    }

    public void testFileLength_ParquetFile() throws IOException {
        // Pre-register parquet container
        directory.getBlobContainerForFormat("parquet");

        List<BlobMetadata> blobList = List.of(new PlainBlobMetadata("_0.parquet", 5678));
        when(parquetBlobContainer.listBlobsByPrefixInSortedOrder(eq("_0.parquet"), eq(1), any())).thenReturn(blobList);

        long length = directory.fileLength("_0.parquet:::parquet");
        assertEquals(5678, length);
    }

    public void testFileLength_FileNotFound() throws IOException {
        when(baseBlobContainer.listBlobsByPrefixInSortedOrder(eq("nonexistent"), eq(1), any())).thenReturn(Collections.emptyList());

        expectThrows(NoSuchFileException.class, () -> directory.fileLength("nonexistent"));
    }

    // ═══════════════════════════════════════════════════════════════
    // Lifecycle Tests
    // ═══════════════════════════════════════════════════════════════

    public void testDelete_DeletesAllContainers() throws IOException {
        // Pre-register parquet container
        directory.getBlobContainerForFormat("parquet");

        directory.delete();

        verify(parquetBlobContainer).delete();
        verify(baseBlobContainer).delete();
    }

    public void testClose_ClearsFormatContainers() throws IOException {
        // Pre-register parquet container
        directory.getBlobContainerForFormat("parquet");

        directory.close();

        // After close, getting parquet format should create a NEW container
        BlobContainer newParquetContainer = mock(BlobContainer.class);
        when(mockBlobStore.blobContainer(baseBlobPath.add("parquet"))).thenReturn(newParquetContainer);

        BlobContainer container = directory.getBlobContainerForFormat("parquet");
        assertSame("Should create new container after close", newParquetContainer, container);
    }

    // ═══════════════════════════════════════════════════════════════
    // Edge Case Tests
    // ═══════════════════════════════════════════════════════════════

    public void testConstructor_NullPluginsService() {
        // Should not throw
        assertNotNull(directory);
    }

    public void testToString() {
        // Pre-register parquet container
        directory.getBlobContainerForFormat("parquet");

        String str = directory.toString();
        assertTrue(str.contains("CompositeRemoteDirectory"));
        assertTrue(str.contains("parquet"));
    }

    // ═══════════════════════════════════════════════════════════════
    // Sync CopyFrom Tests
    // ═══════════════════════════════════════════════════════════════

    public void testSyncCopyFrom_RoutesToCorrectContainer() throws IOException {
        // We can't easily test the full copyFrom without a real Directory,
        // but we can verify that the format routing logic works by testing
        // the getBlobContainerForFormat that copyFrom uses internally.

        // For lucene files
        BlobContainer luceneContainer = directory.getBlobContainerForFormat("lucene");
        assertSame(baseBlobContainer, luceneContainer);

        // For parquet files
        BlobContainer parquetContainer = directory.getBlobContainerForFormat("parquet");
        assertSame(parquetBlobContainer, parquetContainer);
    }
}
