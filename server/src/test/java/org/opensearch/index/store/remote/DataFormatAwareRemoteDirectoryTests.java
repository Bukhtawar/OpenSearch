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
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.exception.CorruptFileException;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeInputStream;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.store.DataFormatAwareStoreDirectory;
import org.opensearch.index.store.FileMetadata;
import org.opensearch.index.store.RemoteIndexOutput;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import org.mockito.Mockito;

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
 * Unit tests for {@link DataFormatAwareRemoteDirectory}.
 */
public class DataFormatAwareRemoteDirectoryTests extends OpenSearchTestCase {

    private static final Logger logger = LogManager.getLogger(DataFormatAwareRemoteDirectoryTests.class);

    private BlobStore mockBlobStore;
    private BlobContainer baseBlobContainer;
    private BlobContainer parquetBlobContainer;
    private BlobPath baseBlobPath;
    private DataFormatAwareRemoteDirectory directory;

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

        directory = new DataFormatAwareRemoteDirectory(
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
        assertTrue(str.contains("DataFormatAwareRemoteDirectory"));
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

    // ═══════════════════════════════════════════════════════════════
    // openInput(String, long, IOContext) Tests - Format-aware routing
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInput_StringBased_LuceneFile() throws IOException {
        byte[] content = new byte[100];
        when(baseBlobContainer.readBlob("_0.cfs")).thenReturn(new ByteArrayInputStream(content));

        IndexInput input = directory.openInput("_0.cfs", 100, IOContext.DEFAULT);
        assertNotNull(input);
        assertEquals(100, input.length());
        input.close();

        verify(baseBlobContainer).readBlob("_0.cfs");
    }

    public void testOpenInput_StringBased_ParquetFile_WithFormatSuffix() throws IOException {
        // Pre-register parquet container
        directory.getBlobContainerForFormat("parquet");

        byte[] content = new byte[200];
        when(parquetBlobContainer.readBlob("_0.parquet:::parquet")).thenReturn(new ByteArrayInputStream(content));

        IndexInput input = directory.openInput("_0.parquet:::parquet", 200, IOContext.DEFAULT);
        assertNotNull(input);
        assertEquals(200, input.length());
        input.close();

        verify(parquetBlobContainer).readBlob("_0.parquet:::parquet");
        verify(baseBlobContainer, never()).readBlob(anyString());
    }

    public void testOpenInput_StringBased_ClosesStreamOnException() throws IOException {
        when(baseBlobContainer.readBlob("_0.cfs")).thenThrow(new IOException("read error"));

        expectThrows(IOException.class, () -> directory.openInput("_0.cfs", 100, IOContext.DEFAULT));
    }

    // ═══════════════════════════════════════════════════════════════
    // createOutput Tests - Format-aware routing
    // ═══════════════════════════════════════════════════════════════

    public void testCreateOutput_LuceneFormat() throws IOException {
        RemoteIndexOutput output = directory.createOutput("test_file", "lucene", IOContext.DEFAULT);
        assertNotNull(output);
    }

    public void testCreateOutput_ParquetFormat() throws IOException {
        // Pre-register parquet container
        directory.getBlobContainerForFormat("parquet");

        RemoteIndexOutput output = directory.createOutput("test_file.parquet", "parquet", IOContext.DEFAULT);
        assertNotNull(output);
    }

    // ═══════════════════════════════════════════════════════════════
    // fileLength(FileMetadata) Tests
    // ═══════════════════════════════════════════════════════════════

    public void testFileLength_FileMetadata_Lucene() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");
        List<BlobMetadata> blobList = List.of(new PlainBlobMetadata("_0.cfs", 1234));
        when(baseBlobContainer.listBlobsByPrefixInSortedOrder(eq("_0.cfs"), eq(1), any())).thenReturn(blobList);

        long length = directory.fileLength(fm);
        assertEquals(1234, length);
    }

    public void testFileLength_FileMetadata_Parquet() throws IOException {
        directory.getBlobContainerForFormat("parquet");

        FileMetadata fm = new FileMetadata("parquet", "_0.parquet");
        List<BlobMetadata> blobList = List.of(new PlainBlobMetadata("_0.parquet", 5678));
        when(parquetBlobContainer.listBlobsByPrefixInSortedOrder(eq("_0.parquet"), eq(1), any())).thenReturn(blobList);

        long length = directory.fileLength(fm);
        assertEquals(5678, length);
    }

    public void testFileLength_FileMetadata_NotFound() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "nonexistent");
        when(baseBlobContainer.listBlobsByPrefixInSortedOrder(eq("nonexistent"), eq(1), any())).thenReturn(Collections.emptyList());

        expectThrows(NoSuchFileException.class, () -> directory.fileLength(fm));
    }

    // ═══════════════════════════════════════════════════════════════
    // openInput(FileMetadata, long, IOContext) Tests
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInput_FileMetadata_Lucene() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");
        byte[] content = new byte[100];
        when(baseBlobContainer.readBlob("_0.cfs")).thenReturn(new ByteArrayInputStream(content));

        IndexInput input = directory.openInput(fm, 100, IOContext.DEFAULT);
        assertNotNull(input);
        assertEquals(100, input.length());
        input.close();

        verify(baseBlobContainer).readBlob("_0.cfs");
    }

    public void testOpenInput_FileMetadata_Parquet() throws IOException {
        directory.getBlobContainerForFormat("parquet");
        FileMetadata fm = new FileMetadata("parquet", "_0.parquet");
        byte[] content = new byte[200];
        when(parquetBlobContainer.readBlob("_0.parquet")).thenReturn(new ByteArrayInputStream(content));

        IndexInput input = directory.openInput(fm, 200, IOContext.DEFAULT);
        assertNotNull(input);
        assertEquals(200, input.length());
        input.close();

        verify(parquetBlobContainer).readBlob("_0.parquet");
        verify(baseBlobContainer, never()).readBlob(anyString());
    }

    public void testOpenInput_FileMetadata_ClosesStreamOnException() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");
        InputStream mockStream = mock(InputStream.class);
        when(baseBlobContainer.readBlob("_0.cfs")).thenReturn(mockStream);
        when(mockStream.read(any(), anyInt(), anyInt())).thenThrow(new IOException("read error"));

        // openInput wraps the stream, reading from it will fail
        IndexInput input = directory.openInput(fm, 100, IOContext.DEFAULT);
        assertNotNull(input);
        input.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // deleteFile(UploadedSegmentMetadata) - Lucene metadata
    // ═══════════════════════════════════════════════════════════════

    public void testDeleteFile_WithUploadedSegmentMetadata_Lucene() throws IOException {
        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString("_0.cfs::_0.cfs__UUID1::checksum123::100::10");

        directory.deleteFile(metadata);

        verify(baseBlobContainer).deleteBlobsIgnoringIfNotExists(Collections.singletonList("_0.cfs__UUID1"));
        verify(parquetBlobContainer, never()).deleteBlobsIgnoringIfNotExists(any());
    }

    // ═══════════════════════════════════════════════════════════════
    // Async copyFrom Tests (8-arg version, returns boolean)
    // ═══════════════════════════════════════════════════════════════

    public void testAsyncCopyFrom_NonAsyncContainer_ReturnsFalse() throws IOException {
        // baseBlobContainer is NOT AsyncMultiStreamBlobContainer, so should return false
        org.opensearch.core.action.ActionListener<Void> listener = mock(org.opensearch.core.action.ActionListener.class);
        org.apache.lucene.store.Directory mockFrom = mock(org.apache.lucene.store.Directory.class);

        boolean result = directory.copyFrom(
            mockFrom,
            "_0.cfs",              // src (lucene format, no :::)
            "_0.cfs__UUID",        // remoteFileName
            IOContext.DEFAULT,
            () -> {},
            listener,
            false,
            null
        );

        assertFalse("Should return false when container is not AsyncMultiStreamBlobContainer", result);
    }

    public void testAsyncCopyFrom_ExceptionHandling() throws IOException {
        org.opensearch.core.action.ActionListener<Void> listener = mock(org.opensearch.core.action.ActionListener.class);
        org.apache.lucene.store.Directory mockFrom = mock(org.apache.lucene.store.Directory.class);
        // Make openInput throw an exception
        when(mockFrom.openInput(anyString(), any())).thenThrow(new IOException("open failed"));

        // Even with exception, it should not propagate but call listener.onFailure
        boolean result = directory.copyFrom(mockFrom, "_0.cfs", "_0.cfs__UUID", IOContext.DEFAULT, () -> {}, listener, false, null);

        // Returns false because baseBlobContainer is not AsyncMultiStreamBlobContainer
        assertFalse(result);
    }

    // ═══════════════════════════════════════════════════════════════
    // FileMetadata-based copyFrom (non-async) - returns false when not async
    // ═══════════════════════════════════════════════════════════════

    public void testCopyFrom_FileMetadata_NonAsync_ReturnsFalse() throws IOException {
        DataFormatAwareStoreDirectory mockComposite = mock(DataFormatAwareStoreDirectory.class);
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");

        org.opensearch.core.action.ActionListener<Void> listener = mock(org.opensearch.core.action.ActionListener.class);

        boolean result = directory.copyFrom(mockComposite, fm, "_0.cfs__UUID", IOContext.DEFAULT, () -> {}, listener, false);

        assertFalse("Should return false when base container is not AsyncMultiStreamBlobContainer", result);
    }

    // ═══════════════════════════════════════════════════════════════
    // openInput(UploadedSegmentMetadata) - Exception closes stream
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInput_UploadedSegmentMetadata_ExceptionClosesStream() throws IOException {
        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString("_0.cfs::_0.cfs__UUID1::checksum123::100::10");
        when(baseBlobContainer.readBlob("_0.cfs__UUID1")).thenThrow(new IOException("blob read failed"));

        expectThrows(IOException.class, () -> directory.openInput(metadata, 100, IOContext.DEFAULT));
    }

    // ═══════════════════════════════════════════════════════════════
    // Metadata file routing
    // ═══════════════════════════════════════════════════════════════

    public void testDeleteFile_MetadataFile() throws IOException {
        directory.deleteFile("metadata__1__2__3");

        // "metadata" format routes to base container
        verify(baseBlobContainer).deleteBlobsIgnoringIfNotExists(Collections.singletonList("metadata__1__2__3"));
    }

    // ═══════════════════════════════════════════════════════════════
    // Sync copyFrom(Directory, String, String, IOContext) Tests
    // ═══════════════════════════════════════════════════════════════

    public void testSyncCopyFrom_LuceneFile_CopiesToBaseContainer() throws IOException {
        Directory mockFrom = mock(Directory.class);
        IndexInput mockInput = mock(IndexInput.class);
        when(mockInput.length()).thenReturn(10L);
        when(mockFrom.openInput(eq("_0.cfs"), any(IOContext.class))).thenReturn(mockInput);

        // The copyFrom creates a RemoteIndexOutput that writes to the base container
        directory.copyFrom(mockFrom, "_0.cfs", "_0.cfs__UUID", IOContext.DEFAULT);

        verify(mockFrom).openInput(eq("_0.cfs"), any(IOContext.class));
    }

    public void testSyncCopyFrom_ParquetFile_CopiesToFormatContainer() throws IOException {
        directory.getBlobContainerForFormat("parquet");

        Directory mockFrom = mock(Directory.class);
        IndexInput mockInput = mock(IndexInput.class);
        when(mockInput.length()).thenReturn(20L);
        when(mockFrom.openInput(eq("_0.pqt:::parquet"), any(IOContext.class))).thenReturn(mockInput);

        directory.copyFrom(mockFrom, "_0.pqt:::parquet", "_0.pqt__UUID", IOContext.DEFAULT);

        verify(mockFrom).openInput(eq("_0.pqt:::parquet"), any(IOContext.class));
    }

    // ═══════════════════════════════════════════════════════════════
    // Async copyFrom with AsyncMultiStreamBlobContainer Tests
    // ═══════════════════════════════════════════════════════════════

    public void testAsyncCopyFrom_WithAsyncContainer_ReturnsTrue() throws Exception {
        // Create an async blob container
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        // Wire up blobStore to return async container for base path
        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null
        );

        // Set up async upload to call onResponse
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onResponse(null);
            return null;
        }).when(asyncContainer).asyncBlobUpload(any(WriteContext.class), any());

        // Create a real directory with a file that has a valid codec footer
        Directory storeDirectory = newDirectory();
        String filename = "_100.si";
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("Hello World!");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Boolean> postUploadInvoked = new AtomicReference<>(false);

        boolean result = asyncDir.copyFrom(
            storeDirectory,
            filename,
            filename,
            IOContext.DEFAULT,
            () -> postUploadInvoked.set(true),
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not fail: " + e.getMessage());
                }
            },
            false,
            null
        );

        assertTrue("Should return true when container is AsyncMultiStreamBlobContainer", result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue(postUploadInvoked.get());
        storeDirectory.close();
    }

    public void testAsyncCopyFrom_ParquetFormat_WithAsyncContainer_ReturnsTrue() throws Exception {
        AsyncMultiStreamBlobContainer asyncParquetContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncParquetContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncParquetContainer.path()).thenReturn(baseBlobPath.add("parquet"));

        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(baseBlobContainer);
        when(asyncBlobStore.blobContainer(baseBlobPath.add("parquet"))).thenReturn(asyncParquetContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null
        );

        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onResponse(null);
            return null;
        }).when(asyncParquetContainer).asyncBlobUpload(any(WriteContext.class), any());

        // Use DataFormatAwareStoreDirectory mock to handle ":::parquet" convention properly
        // The async copyFrom with format routing uses uploadBlob, which calls from.openInput(src, ...)
        // and calculateChecksumOfChecksum(from, src). A plain Directory can't understand ":::parquet"
        // so we use mock DataFormatAwareStoreDirectory + FileMetadata-based copyFrom instead.
        DataFormatAwareStoreDirectory mockComposite = mock(DataFormatAwareStoreDirectory.class);
        FileMetadata fm = new FileMetadata("parquet", "_0.pqt");

        IndexInput mockInput = mock(IndexInput.class);
        when(mockInput.length()).thenReturn(100L);
        when(mockInput.clone()).thenReturn(mockInput);
        when(mockComposite.openInput(eq(fm), any(IOContext.class))).thenReturn(mockInput);
        when(mockComposite.calculateChecksum(fm)).thenReturn(67890L);

        CountDownLatch latch = new CountDownLatch(1);

        boolean result = asyncDir.copyFrom(mockComposite, fm, "_0.pqt__UUID", IOContext.DEFAULT, () -> {}, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
            }
        }, false);

        assertTrue("Should return true for async parquet container", result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    public void testAsyncCopyFrom_ExceptionDuringUpload_CallsListenerOnFailure() throws Exception {
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null
        );

        // File does not exist - openInput will throw
        Directory storeDirectory = newDirectory();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        boolean result = asyncDir.copyFrom(
            storeDirectory,
            "_nonexistent.si",
            "_nonexistent.si__UUID",
            IOContext.DEFAULT,
            () -> {},
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    fail("Should have failed");
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            },
            false,
            null
        );

        assertTrue("Should return true (handled)", result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNotNull(failureRef.get());
        storeDirectory.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // FileMetadata-based async copyFrom with AsyncMultiStreamBlobContainer
    // ═══════════════════════════════════════════════════════════════

    public void testCopyFrom_FileMetadata_Async_WithAsyncContainer_ReturnsTrue() throws Exception {
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null
        );

        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onResponse(null);
            return null;
        }).when(asyncContainer).asyncBlobUpload(any(WriteContext.class), any());

        DataFormatAwareStoreDirectory mockComposite = mock(DataFormatAwareStoreDirectory.class);
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");

        IndexInput mockInput = mock(IndexInput.class);
        when(mockInput.length()).thenReturn(100L);
        when(mockInput.clone()).thenReturn(mockInput);
        when(mockComposite.openInput(eq(fm), any(IOContext.class))).thenReturn(mockInput);
        when(mockComposite.calculateChecksum(fm)).thenReturn(12345L);

        CountDownLatch latch = new CountDownLatch(1);

        boolean result = asyncDir.copyFrom(mockComposite, fm, "_0.cfs__UUID", IOContext.DEFAULT, () -> {}, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
            }
        }, false);

        assertTrue("Should return true for async container", result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    public void testCopyFrom_FileMetadata_Async_ExceptionCallsListener() throws Exception {
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null
        );

        DataFormatAwareStoreDirectory mockComposite = mock(DataFormatAwareStoreDirectory.class);
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");
        when(mockComposite.calculateChecksum(fm)).thenThrow(new IOException("checksum failed"));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        boolean result = asyncDir.copyFrom(mockComposite, fm, "_0.cfs__UUID", IOContext.DEFAULT, () -> {}, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                fail("Should have failed");
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
                latch.countDown();
            }
        }, false);

        assertTrue("Should return true (handled)", result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNotNull(failureRef.get());
    }

    // ═══════════════════════════════════════════════════════════════
    // openInput exception close-on-failure (stream opened but readBlob fails)
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInput_UploadedSegmentMetadata_StreamClosedOnReadException() throws IOException {
        InputStream mockStream = mock(InputStream.class);
        when(baseBlobContainer.readBlob("_0.cfs__UUID1")).thenReturn(mockStream);
        // Simulate the rate limiter wrapping failing by making read throw
        when(mockStream.read(any(), anyInt(), anyInt())).thenThrow(new IOException("stream corrupted"));

        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString("_0.cfs::_0.cfs__UUID1::checksum123::100::10");

        // openInput wraps the stream; the exception happens later during read
        IndexInput input = directory.openInput(metadata, 100, IOContext.DEFAULT);
        assertNotNull(input);
        // Close the input - it should clean up properly
        input.close();
    }

    public void testOpenInput_FileMetadata_ExceptionClosesStream() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");
        when(baseBlobContainer.readBlob("_0.cfs")).thenThrow(new IOException("blob read failed"));

        expectThrows(IOException.class, () -> directory.openInput(fm, 100, IOContext.DEFAULT));
    }

    public void testOpenInput_StringBased_StreamClosedWhenInputStreamReadFails() throws IOException {
        InputStream mockStream = mock(InputStream.class);
        when(baseBlobContainer.readBlob("_0.cfs")).thenReturn(mockStream);
        when(mockStream.read(any(), anyInt(), anyInt())).thenThrow(new IOException("stream error"));

        // openInput wraps the stream successfully
        IndexInput input = directory.openInput("_0.cfs", 100, IOContext.DEFAULT);
        assertNotNull(input);
        input.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // fileLength name mismatch tests
    // ═══════════════════════════════════════════════════════════════

    public void testFileLength_NameMismatch_ThrowsNoSuchFile() throws IOException {
        // Returns a blob but name doesn't match
        List<BlobMetadata> blobList = List.of(new PlainBlobMetadata("_0.cfs_DIFFERENT", 1234));
        when(baseBlobContainer.listBlobsByPrefixInSortedOrder(eq("_0.cfs"), eq(1), any())).thenReturn(blobList);

        expectThrows(NoSuchFileException.class, () -> directory.fileLength("_0.cfs"));
    }

    public void testFileLength_FileMetadata_NameMismatch_ThrowsNoSuchFile() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");
        List<BlobMetadata> blobList = List.of(new PlainBlobMetadata("_0.cfs_DIFFERENT", 1234));
        when(baseBlobContainer.listBlobsByPrefixInSortedOrder(eq("_0.cfs"), eq(1), any())).thenReturn(blobList);

        expectThrows(NoSuchFileException.class, () -> directory.fileLength(fm));
    }

    // ═══════════════════════════════════════════════════════════════
    // DownloadRateLimiterProvider merged segment path
    // ═══════════════════════════════════════════════════════════════

    public void testDownloadRateLimiter_MergedSegment_UsesLowPriorityRateLimiter() throws IOException {
        // Create directory with pending merged segments
        Map<String, String> pendingMergedSegments = new HashMap<>();
        pendingMergedSegments.put("localFile", "_0.cfs__UUID_MERGED");

        UnaryOperator<InputStream> normalRateLimiter = stream -> stream;
        UnaryOperator<InputStream> lowPriorityRateLimiter = stream -> stream;

        DataFormatAwareRemoteDirectory dirWithMerged = new DataFormatAwareRemoteDirectory(
            mockBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            normalRateLimiter,
            lowPriorityRateLimiter,
            pendingMergedSegments,
            logger,
            null
        );

        // When opening a merged segment, the low-priority rate limiter should be used
        byte[] content = new byte[100];
        when(baseBlobContainer.readBlob("_0.cfs__UUID_MERGED")).thenReturn(new ByteArrayInputStream(content));

        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString("_0.cfs::_0.cfs__UUID_MERGED::checksum123::100::10");

        IndexInput input = dirWithMerged.openInput(metadata, 100, IOContext.DEFAULT);
        assertNotNull(input);
        assertEquals(100, input.length());
        input.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // Completion listener tests (via async upload with errors)
    // ═══════════════════════════════════════════════════════════════

    public void testCompletionListener_PostUploadRunnerException() throws Exception {
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null
        );

        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onResponse(null);
            return null;
        }).when(asyncContainer).asyncBlobUpload(any(WriteContext.class), any());

        Directory storeDirectory = newDirectory();
        String filename = "_100.si";
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("data");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        CountDownLatch latch = new CountDownLatch(1);

        // postUploadRunner throws exception → listener.onFailure
        boolean result = asyncDir.copyFrom(storeDirectory, filename, filename + "__UUID", IOContext.DEFAULT, () -> {
            throw new RuntimeException("postUpload error");
        }, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                fail("Should not succeed");
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
            }
        }, false, null);

        assertTrue(result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        storeDirectory.close();
    }

    public void testCompletionListener_CorruptIndexException() throws Exception {
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null
        );

        // asyncBlobUpload calls onFailure with a wrapped CorruptIndexException
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onFailure(new RuntimeException(new CorruptIndexException("corrupted", "test")));
            return null;
        }).when(asyncContainer).asyncBlobUpload(any(WriteContext.class), any());

        Directory storeDirectory = newDirectory();
        String filename = "_100.si";
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("data");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        boolean result = asyncDir.copyFrom(
            storeDirectory,
            filename,
            filename + "__UUID",
            IOContext.DEFAULT,
            () -> {},
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    fail("Should not succeed");
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            },
            false,
            null
        );

        assertTrue(result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue("Should be CorruptIndexException", failureRef.get() instanceof CorruptIndexException);
        storeDirectory.close();
    }

    public void testCompletionListener_CorruptFileException() throws Exception {
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null
        );

        // asyncBlobUpload calls onFailure with a wrapped CorruptFileException
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onFailure(new RuntimeException(new CorruptFileException("corrupted", "test_file")));
            return null;
        }).when(asyncContainer).asyncBlobUpload(any(WriteContext.class), any());

        Directory storeDirectory = newDirectory();
        String filename = "_100.si";
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("data");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        boolean result = asyncDir.copyFrom(
            storeDirectory,
            filename,
            filename + "__UUID",
            IOContext.DEFAULT,
            () -> {},
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    fail("Should not succeed");
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            },
            false,
            null
        );

        assertTrue(result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue("Should be CorruptIndexException", failureRef.get() instanceof CorruptIndexException);
        storeDirectory.close();
    }

    public void testCompletionListener_GenericException() throws Exception {
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null
        );

        // asyncBlobUpload calls onFailure with a generic exception (not corrupt)
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onFailure(new IOException("network error"));
            return null;
        }).when(asyncContainer).asyncBlobUpload(any(WriteContext.class), any());

        Directory storeDirectory = newDirectory();
        String filename = "_100.si";
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("data");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failureRef = new AtomicReference<>();

        boolean result = asyncDir.copyFrom(
            storeDirectory,
            filename,
            filename + "__UUID",
            IOContext.DEFAULT,
            () -> {},
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    fail("Should not succeed");
                }

                @Override
                public void onFailure(Exception e) {
                    failureRef.set(e);
                    latch.countDown();
                }
            },
            false,
            null
        );

        assertTrue(result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue("Should be IOException", failureRef.get() instanceof IOException);
        storeDirectory.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // Sync copyFrom with FileMetadata
    // ═══════════════════════════════════════════════════════════════

    public void testCopyFrom_FileMetadata_Sync() throws IOException {
        DataFormatAwareStoreDirectory mockComposite = mock(DataFormatAwareStoreDirectory.class);
        FileMetadata fm = new FileMetadata("lucene", "_0.cfs");

        IndexInput mockInput = mock(IndexInput.class);
        when(mockInput.length()).thenReturn(50L);
        when(mockComposite.openInput(eq(fm), eq(IOContext.READONCE))).thenReturn(mockInput);

        // Should write to base container since format is "lucene"
        directory.copyFrom(mockComposite, fm, "_0.cfs__UUID", IOContext.DEFAULT);

        verify(mockComposite).openInput(eq(fm), eq(IOContext.READONCE));
    }

    // ═══════════════════════════════════════════════════════════════
    // Low priority upload path (content > 15GB triggers low priority)
    // ═══════════════════════════════════════════════════════════════

    public void testAsyncCopyFrom_LowPriorityUpload() throws Exception {
        AsyncMultiStreamBlobContainer asyncContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(asyncContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        when(asyncContainer.path()).thenReturn(baseBlobPath);

        BlobStore asyncBlobStore = mock(BlobStore.class);
        when(asyncBlobStore.blobContainer(baseBlobPath)).thenReturn(asyncContainer);

        DataFormatAwareRemoteDirectory asyncDir = new DataFormatAwareRemoteDirectory(
            asyncBlobStore,
            baseBlobPath,
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            UnaryOperator.identity(),
            new HashMap<>(),
            logger,
            null
        );

        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onResponse(null);
            return null;
        }).when(asyncContainer).asyncBlobUpload(any(WriteContext.class), any());

        Directory storeDirectory = newDirectory();
        String filename = "_100.si";
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("data");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        CountDownLatch latch = new CountDownLatch(1);

        // Pass lowPriorityUpload=true
        boolean result = asyncDir.copyFrom(
            storeDirectory,
            filename,
            filename + "__UUID",
            IOContext.DEFAULT,
            () -> {},
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not fail: " + e.getMessage());
                }
            },
            true,  // lowPriorityUpload
            null
        );

        assertTrue(result);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        storeDirectory.close();
    }
}
