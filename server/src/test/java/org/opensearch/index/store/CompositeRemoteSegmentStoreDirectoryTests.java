/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.IOContext;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.store.lockmanager.RemoteStoreMetadataLockManager;
import org.opensearch.index.store.remote.CompositeRemoteDirectory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompositeRemoteSegmentStoreDirectoryTests extends OpenSearchTestCase {

    private CompositeRemoteDirectory compositeRemoteDirectory;
    private RemoteDirectory remoteMetadataDirectory;
    private RemoteStoreMetadataLockManager mdLockManager;
    private ThreadPool threadPool;
    private ShardId shardId;
    private CompositeRemoteSegmentStoreDirectory directory;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        compositeRemoteDirectory = mock(CompositeRemoteDirectory.class);
        remoteMetadataDirectory = mock(RemoteDirectory.class);
        mdLockManager = mock(RemoteStoreMetadataLockManager.class);
        threadPool = mock(ThreadPool.class);
        shardId = new ShardId(new Index("test-index", "test-uuid"), 0);

        ExecutorService executorService = OpenSearchExecutors.newDirectExecutorService();
        when(threadPool.executor(ThreadPool.Names.REMOTE_PURGE)).thenReturn(executorService);
        when(threadPool.executor(ThreadPool.Names.REMOTE_RECOVERY)).thenReturn(executorService);
        when(threadPool.executor(ThreadPool.Names.SAME)).thenReturn(executorService);

        // Return empty metadata files so init() doesn't fail
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.METADATA_PREFIX,
                CompositeRemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH
            )
        ).thenReturn(Collections.emptyList());

        directory = new CompositeRemoteSegmentStoreDirectory(
            compositeRemoteDirectory,
            remoteMetadataDirectory,
            mdLockManager,
            threadPool,
            shardId,
            null
        );
    }

    // ═══════════════════════════════════════════════════════════════
    // init()
    // ═══════════════════════════════════════════════════════════════

    public void testInit_noMetadataFile() throws IOException {
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.METADATA_PREFIX,
                CompositeRemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH
            )
        ).thenReturn(Collections.emptyList());

        directory.init();

        Map<String, UploadedSegmentMetadata> cache = directory.getSegmentsUploadedToRemoteStore();
        assertTrue("Cache should be empty when no metadata files exist", cache.isEmpty());
    }

    // ═══════════════════════════════════════════════════════════════
    // copyFrom - requires CompositeStoreDirectory
    // ═══════════════════════════════════════════════════════════════

    public void testCopyFrom_nonCompositeStoreDirectoryThrows() {
        // Use a mock of regular Directory (not CompositeStoreDirectory)
        org.apache.lucene.store.Directory regularDirectory = mock(org.apache.lucene.store.Directory.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        expectThrows(
            IllegalArgumentException.class,
            () -> directory.copyFrom(regularDirectory, "_0.si", IOContext.DEFAULT, listener, false)
        );
    }

    public void testCopyFrom_withCompositeStoreDirectory_asyncUpload() throws IOException {
        CompositeStoreDirectory storeDirectory = mock(CompositeStoreDirectory.class);
        FileMetadata fileMetadata = new FileMetadata("lucene", "_0.si");

        // Mock the compositeRemoteDirectory to indicate async upload succeeded
        when(
            compositeRemoteDirectory.copyFrom(
                eq(storeDirectory),
                any(FileMetadata.class),
                any(String.class),
                eq(IOContext.DEFAULT),
                any(Runnable.class),
                any(ActionListener.class),
                eq(false)
            )
        ).thenReturn(true);

        ActionListener<Void> listener = mock(ActionListener.class);

        directory.copyFrom(storeDirectory, fileMetadata, IOContext.DEFAULT, listener, false);

        verify(compositeRemoteDirectory).copyFrom(
            eq(storeDirectory),
            any(FileMetadata.class),
            any(String.class),
            eq(IOContext.DEFAULT),
            any(Runnable.class),
            any(ActionListener.class),
            eq(false)
        );
    }

    public void testCopyFrom_withCompositeStoreDirectory_fallbackSync() throws IOException {
        CompositeStoreDirectory storeDirectory = mock(CompositeStoreDirectory.class);
        FileMetadata fileMetadata = new FileMetadata("lucene", "_0.si");

        // Mock: async upload returns false (not supported)
        when(
            compositeRemoteDirectory.copyFrom(
                eq(storeDirectory),
                any(FileMetadata.class),
                any(String.class),
                eq(IOContext.DEFAULT),
                any(Runnable.class),
                any(ActionListener.class),
                eq(false)
            )
        ).thenReturn(false);

        // Mock the sync copyFrom path
        when(storeDirectory.getChecksumOfLocalFile(any(FileMetadata.class))).thenReturn(12345L);
        when(storeDirectory.fileLength(any(FileMetadata.class))).thenReturn(100L);

        ActionListener<Void> listener = mock(ActionListener.class);

        directory.copyFrom(storeDirectory, fileMetadata, IOContext.DEFAULT, listener, false);

        // Should call listener.onResponse after sync fallback
        verify(listener).onResponse(null);
    }

    // ═══════════════════════════════════════════════════════════════
    // containsFile
    // ═══════════════════════════════════════════════════════════════

    public void testContainsFile_checksumMatch() throws IOException {
        // Populate cache via a manual upload simulation
        CompositeStoreDirectory storeDir = mock(CompositeStoreDirectory.class);
        FileMetadata fm = new FileMetadata("lucene", "_0.si");

        when(
            compositeRemoteDirectory.copyFrom(
                eq(storeDir),
                any(FileMetadata.class),
                any(String.class),
                any(IOContext.class),
                any(Runnable.class),
                any(ActionListener.class),
                eq(false)
            )
        ).thenAnswer(invocation -> {
            Runnable postUploadRunner = invocation.getArgument(4);
            postUploadRunner.run();
            ActionListener<Void> listener = invocation.getArgument(5);
            listener.onResponse(null);
            return true;
        });

        when(storeDir.calculateUploadChecksum(any(FileMetadata.class))).thenReturn("12345");
        when(storeDir.fileLength(any(FileMetadata.class))).thenReturn(100L);
        when(storeDir.getChecksumOfLocalFile(any(FileMetadata.class))).thenReturn(12345L);

        ActionListener<Void> listener = mock(ActionListener.class);
        directory.copyFrom(fm, storeDir, IOContext.DEFAULT, listener, false);

        assertTrue(directory.containsFile(fm, "12345"));
    }

    public void testContainsFile_checksumMismatch() {
        // The cache is empty at this point, so any containsFile should return false
        FileMetadata fm = new FileMetadata("lucene", "_0.si");
        assertFalse(directory.containsFile(fm, "99999"));
    }

    public void testContainsFile_notPresent() {
        FileMetadata fm = new FileMetadata("lucene", "_nonexistent.si");
        assertFalse(directory.containsFile(fm, "12345"));
    }

    public void testContainsFile_stringOverload() {
        // The string overload should work the same way
        assertFalse(directory.containsFile("_0.si", "12345"));
    }

    // ═══════════════════════════════════════════════════════════════
    // fileLength
    // ═══════════════════════════════════════════════════════════════

    public void testFileLength_fromRemote() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "_remote.si");
        when(compositeRemoteDirectory.fileLength(fm)).thenReturn(2048L);

        long length = directory.fileLength(fm);
        assertEquals(2048L, length);
    }

    public void testFileLength_notFound_throws() throws IOException {
        FileMetadata fm = new FileMetadata("lucene", "_missing.si");
        when(compositeRemoteDirectory.fileLength(fm)).thenThrow(new NoSuchFileException("not found"));

        expectThrows(NoSuchFileException.class, () -> directory.fileLength(fm));
    }

    public void testFileLength_stringOverload() throws IOException {
        // String overload wraps the string in FileMetadata
        when(compositeRemoteDirectory.fileLength(any(FileMetadata.class))).thenReturn(512L);

        long length = directory.fileLength("_0.si:::lucene");
        assertEquals(512L, length);
    }

    // ═══════════════════════════════════════════════════════════════
    // getExistingRemoteFilename
    // ═══════════════════════════════════════════════════════════════

    public void testGetExistingRemoteFilename_missing() {
        FileMetadata fm = new FileMetadata("lucene", "_nonexistent.si");
        assertNull(directory.getExistingRemoteFilename(fm));
    }

    public void testGetExistingRemoteFilename_fromPendingDownload() throws IOException {
        // Create a directory with pending download segments
        Map<FileMetadata, String> pendingSegments = new ConcurrentHashMap<>();
        FileMetadata fm = new FileMetadata("lucene", "_pending.si");
        pendingSegments.put(fm, "_pending.si__remote_uuid");

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.METADATA_PREFIX,
                CompositeRemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH
            )
        ).thenReturn(Collections.emptyList());

        CompositeRemoteSegmentStoreDirectory dirWithPending = new CompositeRemoteSegmentStoreDirectory(
            compositeRemoteDirectory,
            remoteMetadataDirectory,
            mdLockManager,
            threadPool,
            shardId,
            pendingSegments
        );

        String remoteFilename = dirWithPending.getExistingRemoteFilename(fm);
        assertEquals("_pending.si__remote_uuid", remoteFilename);
    }

    // ═══════════════════════════════════════════════════════════════
    // isMergedSegmentPendingDownload / unmark
    // ═══════════════════════════════════════════════════════════════

    public void testIsMergedSegmentPendingDownload() throws IOException {
        Map<FileMetadata, String> pendingSegments = new ConcurrentHashMap<>();
        FileMetadata fm = new FileMetadata("lucene", "_merged.si");
        pendingSegments.put(fm, "_merged.si__uuid");

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.METADATA_PREFIX,
                CompositeRemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH
            )
        ).thenReturn(Collections.emptyList());

        CompositeRemoteSegmentStoreDirectory dirWithPending = new CompositeRemoteSegmentStoreDirectory(
            compositeRemoteDirectory,
            remoteMetadataDirectory,
            mdLockManager,
            threadPool,
            shardId,
            pendingSegments
        );

        assertTrue(dirWithPending.isMergedSegmentPendingDownload(fm));
        assertFalse(dirWithPending.isMergedSegmentPendingDownload(new FileMetadata("lucene", "_other.si")));
    }

    public void testUnmarkMergedSegmentsPendingDownload() throws IOException {
        Map<FileMetadata, String> pendingSegments = new ConcurrentHashMap<>();
        FileMetadata fm1 = new FileMetadata("lucene", "_1.si");
        FileMetadata fm2 = new FileMetadata("lucene", "_2.si");
        pendingSegments.put(fm1, "_1.si__uuid");
        pendingSegments.put(fm2, "_2.si__uuid");

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.METADATA_PREFIX,
                CompositeRemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH
            )
        ).thenReturn(Collections.emptyList());

        CompositeRemoteSegmentStoreDirectory dirWithPending = new CompositeRemoteSegmentStoreDirectory(
            compositeRemoteDirectory,
            remoteMetadataDirectory,
            mdLockManager,
            threadPool,
            shardId,
            pendingSegments
        );

        assertTrue(dirWithPending.isMergedSegmentPendingDownload(fm1));
        assertTrue(dirWithPending.isMergedSegmentPendingDownload(fm2));

        dirWithPending.unmarkMergedSegmentsPendingDownload(Set.of("_1.si"));

        // fm1 should be unmarked (removed from pending), fm2 should still be pending
        assertFalse("fm1 should no longer be pending after unmark", dirWithPending.isMergedSegmentPendingDownload(fm1));
        assertTrue("fm2 should still be pending", dirWithPending.isMergedSegmentPendingDownload(fm2));
    }

    public void testGetSegmentsUploadedToRemoteStore_empty() {
        Map<String, UploadedSegmentMetadata> cache = directory.getSegmentsUploadedToRemoteStore();
        assertTrue("Cache should be empty initially", cache.isEmpty());
    }

    // ═══════════════════════════════════════════════════════════════
    // delete
    // ═══════════════════════════════════════════════════════════════

    public void testDelete_delegatesToChildren() throws IOException {
        boolean result = directory.delete();

        verify(compositeRemoteDirectory).delete();
        verify(remoteMetadataDirectory).delete();
        verify(mdLockManager).delete();
        assertTrue(result);
    }

    public void testDelete_exceptionReturnsFalse() throws IOException {
        org.mockito.Mockito.doThrow(new IOException("delete failed")).when(compositeRemoteDirectory).delete();

        boolean result = directory.delete();
        assertFalse(result);
    }

    // ═══════════════════════════════════════════════════════════════
    // canDeleteStaleCommits
    // ═══════════════════════════════════════════════════════════════

    public void testCanDeleteStaleCommits_initiallyTrue() {
        assertTrue(directory.canDeleteStaleCommits.get());
    }

    public void testDeleteStaleSegmentsAsync_setsCanDeleteToFalse() throws Exception {
        when(remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(MetadataFilenameUtils.METADATA_PREFIX, Integer.MAX_VALUE))
            .thenReturn(Collections.emptyList());

        directory.deleteStaleSegmentsAsync(5);

        // After completion, should be set back to true
        assertBusy(() -> assertTrue(directory.canDeleteStaleCommits.get()));
    }

    public void testDeleteStaleSegmentsAsync_alreadyInProgress() {
        directory.canDeleteStaleCommits.set(false);

        directory.deleteStaleSegmentsAsync(2);

        // Should remain false since it didn't run
        assertFalse(directory.canDeleteStaleCommits.get());
    }

    // ═══════════════════════════════════════════════════════════════
    // getMetadataFilesForActiveSegments
    // ═══════════════════════════════════════════════════════════════

    public void testGetMetadataFilesForActiveSegments_noLocked() {
        List<String> sortedMdFiles = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            sortedMdFiles.add("metadata__" + i);
        }
        Set<String> lockedFiles = new HashSet<>();
        int lastNToKeep = 3;

        Set<String> result = directory.getMetadataFilesToFilterActiveSegments(lastNToKeep, sortedMdFiles, lockedFiles);

        // Files at index 3 and 4 are eligible for deletion (indices 0,1,2 are kept)
        // The method adds the "previous" file (index 2) since it's the boundary
        assertTrue(result.contains(sortedMdFiles.get(2)));
    }

    public void testGetMetadataFilesForActiveSegments_allLocked() {
        List<String> sortedMdFiles = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            sortedMdFiles.add("metadata__" + i);
        }
        Set<String> lockedFiles = new HashSet<>(sortedMdFiles);

        Set<String> result = directory.getMetadataFilesToFilterActiveSegments(0, sortedMdFiles, lockedFiles);
        assertTrue("When all files are locked, no filtering needed", result.isEmpty());
    }

    // ═══════════════════════════════════════════════════════════════
    // Constructor - null pendingDownloadMergedSegments
    // ═══════════════════════════════════════════════════════════════

    public void testConstructor_nullPendingSegments() throws IOException {
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.METADATA_PREFIX,
                CompositeRemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH
            )
        ).thenReturn(Collections.emptyList());

        // Should not throw even with null pendingDownloadMergedSegments
        CompositeRemoteSegmentStoreDirectory dir = new CompositeRemoteSegmentStoreDirectory(
            compositeRemoteDirectory,
            remoteMetadataDirectory,
            mdLockManager,
            threadPool,
            shardId,
            null
        );
        assertNotNull(dir);
    }

    public void testConstructor_nonNullPendingSegments() throws IOException {
        Map<FileMetadata, String> pendingSegments = new ConcurrentHashMap<>();
        pendingSegments.put(new FileMetadata("lucene", "_0.si"), "_0.si__uuid");

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.METADATA_PREFIX,
                CompositeRemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH
            )
        ).thenReturn(Collections.emptyList());

        CompositeRemoteSegmentStoreDirectory dir = new CompositeRemoteSegmentStoreDirectory(
            compositeRemoteDirectory,
            remoteMetadataDirectory,
            mdLockManager,
            threadPool,
            shardId,
            pendingSegments
        );
        assertNotNull(dir);
    }

    // ═══════════════════════════════════════════════════════════════
    // Lock operations
    // ═══════════════════════════════════════════════════════════════

    public void testAcquireLock() throws IOException {
        long primaryTerm = 1;
        long generation = 5;
        String acquirerId = "test-acquirer";

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, generation),
                1
            )
        ).thenReturn(List.of("metadata__1__5__abc"));

        directory.acquireLock(primaryTerm, generation, acquirerId);
        verify(mdLockManager).acquire(any());
    }

    public void testReleaseLock() throws IOException {
        long primaryTerm = 1;
        long generation = 5;
        String acquirerId = "test-acquirer";

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, generation),
                1
            )
        ).thenReturn(List.of("metadata__1__5__abc"));

        directory.releaseLock(primaryTerm, generation, acquirerId);
        verify(mdLockManager).release(any());
    }

    public void testIsLockAcquired() throws IOException {
        long primaryTerm = 1;
        long generation = 5;

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, generation),
                1
            )
        ).thenReturn(List.of("metadata__1__5__abc"));

        directory.isLockAcquired(primaryTerm, generation);
        verify(mdLockManager).isAcquired(any());
    }

    public void testAcquireLock_noMetadataFile_throws() throws IOException {
        long primaryTerm = 2;
        long generation = 3;

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, generation),
                1
            )
        ).thenReturn(Collections.emptyList());

        expectThrows(NoSuchFileException.class, () -> directory.acquireLock(primaryTerm, generation, "test-acquirer"));
    }

    // ═══════════════════════════════════════════════════════════════
    // getMetadataFileForCommit
    // ═══════════════════════════════════════════════════════════════

    public void testGetMetadataFileForCommit() throws IOException {
        long primaryTerm = 2;
        long generation = 3;
        String expectedFile = "metadata__2__3__pqr";

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, generation),
                1
            )
        ).thenReturn(List.of(expectedFile));

        String result = directory.getMetadataFileForCommit(primaryTerm, generation);
        assertEquals(expectedFile, result);
    }

    public void testGetMetadataFileForCommit_noFile_throws() throws IOException {
        long primaryTerm = 2;
        long generation = 3;

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, generation),
                1
            )
        ).thenReturn(Collections.emptyList());

        expectThrows(NoSuchFileException.class, () -> directory.getMetadataFileForCommit(primaryTerm, generation));
    }

    // ═══════════════════════════════════════════════════════════════
    // uploadMetadata - requires CompositeStoreDirectory
    // ═══════════════════════════════════════════════════════════════

    public void testUploadMetadata_nonCompositeStoreDirectory_throws() {
        org.apache.lucene.store.Directory regularDirectory = mock(org.apache.lucene.store.Directory.class);

        expectThrows(
            IllegalArgumentException.class,
            () -> directory.uploadMetadata(List.of("_0.si"), (CatalogSnapshot) null, regularDirectory, 34L, null, "node-1")
        );
    }

    // ═══════════════════════════════════════════════════════════════
    // removeFileFromSegmentsUploadedToRemoteStore
    // ═══════════════════════════════════════════════════════════════

    public void testRemoveFileFromSegmentsUploadedToRemoteStore() throws IOException {
        // We need to add a file to the cache first, then remove it
        CompositeStoreDirectory storeDir = mock(CompositeStoreDirectory.class);
        FileMetadata fm = new FileMetadata("lucene", "_remove_test.si");

        // Mock async upload to succeed and populate cache
        when(
            compositeRemoteDirectory.copyFrom(
                eq(storeDir),
                any(FileMetadata.class),
                any(String.class),
                any(IOContext.class),
                any(Runnable.class),
                any(ActionListener.class),
                eq(false)
            )
        ).thenAnswer(invocation -> {
            Runnable postUploadRunner = invocation.getArgument(4);
            postUploadRunner.run();
            ActionListener<Void> listener = invocation.getArgument(5);
            listener.onResponse(null);
            return true;
        });

        when(storeDir.calculateUploadChecksum(any(FileMetadata.class))).thenReturn("54321");
        when(storeDir.fileLength(any(FileMetadata.class))).thenReturn(200L);

        ActionListener<Void> listener = mock(ActionListener.class);
        directory.copyFrom(fm, storeDir, IOContext.DEFAULT, listener, false);

        // Verify file is in cache
        assertTrue(directory.containsFile(fm, "54321"));

        // Now remove it
        UploadedSegmentMetadata metadata = new UploadedSegmentMetadata("_remove_test.si", "_remove_test.si__uuid", "54321", 200, "lucene");
        directory.removeFileFromSegmentsUploadedToRemoteStore(metadata);

        // Verify file is removed from cache
        assertFalse(directory.containsFile(fm, "54321"));
    }

    // ═══════════════════════════════════════════════════════════════
    // copyFrom deprecated string-based
    // ═══════════════════════════════════════════════════════════════

    public void testCopyFrom_deprecated_throws() {
        org.apache.lucene.store.Directory dir = mock(org.apache.lucene.store.Directory.class);
        expectThrows(UnsupportedOperationException.class, () -> directory.copyFrom(dir, "_0.si", "_0.si", IOContext.DEFAULT));
    }

    // ═══════════════════════════════════════════════════════════════
    // fileLength - from cache (uploaded segments)
    // ═══════════════════════════════════════════════════════════════

    public void testFileLength_fromCache() throws IOException {
        // Upload a file to populate the cache
        CompositeStoreDirectory storeDir = mock(CompositeStoreDirectory.class);
        FileMetadata fm = new FileMetadata("lucene", "_cached.si");

        when(
            compositeRemoteDirectory.copyFrom(
                eq(storeDir),
                any(FileMetadata.class),
                any(String.class),
                any(IOContext.class),
                any(Runnable.class),
                any(ActionListener.class),
                eq(false)
            )
        ).thenAnswer(invocation -> {
            Runnable postUploadRunner = invocation.getArgument(4);
            postUploadRunner.run();
            ActionListener<Void> listener = invocation.getArgument(5);
            listener.onResponse(null);
            return true;
        });

        when(storeDir.calculateUploadChecksum(any(FileMetadata.class))).thenReturn("11111");
        when(storeDir.fileLength(any(FileMetadata.class))).thenReturn(4096L);

        ActionListener<Void> listener = mock(ActionListener.class);
        directory.copyFrom(fm, storeDir, IOContext.DEFAULT, listener, false);

        // fileLength should return the cached value (4096) without calling compositeRemoteDirectory
        long length = directory.fileLength(fm);
        assertEquals(4096L, length);
    }

    // ═══════════════════════════════════════════════════════════════
    // containsFile after upload with correct and wrong checksums
    // ═══════════════════════════════════════════════════════════════

    public void testContainsFile_afterUpload_wrongChecksum() throws IOException {
        CompositeStoreDirectory storeDir = mock(CompositeStoreDirectory.class);
        FileMetadata fm = new FileMetadata("parquet", "_data.parquet");

        when(
            compositeRemoteDirectory.copyFrom(
                eq(storeDir),
                any(FileMetadata.class),
                any(String.class),
                any(IOContext.class),
                any(Runnable.class),
                any(ActionListener.class),
                eq(false)
            )
        ).thenAnswer(invocation -> {
            Runnable postUploadRunner = invocation.getArgument(4);
            postUploadRunner.run();
            ActionListener<Void> listener = invocation.getArgument(5);
            listener.onResponse(null);
            return true;
        });

        when(storeDir.calculateUploadChecksum(any(FileMetadata.class))).thenReturn("99999");
        when(storeDir.fileLength(any(FileMetadata.class))).thenReturn(500L);

        ActionListener<Void> listener = mock(ActionListener.class);
        directory.copyFrom(fm, storeDir, IOContext.DEFAULT, listener, false);

        // Correct checksum should match
        assertTrue(directory.containsFile(fm, "99999"));
        // Wrong checksum should not match
        assertFalse(directory.containsFile(fm, "00000"));
    }

    // ═══════════════════════════════════════════════════════════════
    // copyFrom with string-based overload dispatches to FileMetadata
    // ═══════════════════════════════════════════════════════════════

    public void testCopyFrom_stringOverload_delegatesToCompositeStoreDirectory() {
        CompositeStoreDirectory storeDirectory = mock(CompositeStoreDirectory.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        // Should dispatch through to the FileMetadata-based path
        when(
            compositeRemoteDirectory.copyFrom(
                eq(storeDirectory),
                any(FileMetadata.class),
                any(String.class),
                eq(IOContext.DEFAULT),
                any(Runnable.class),
                any(ActionListener.class),
                eq(false)
            )
        ).thenReturn(true);

        directory.copyFrom(storeDirectory, "_0.si", IOContext.DEFAULT, listener, false);

        verify(compositeRemoteDirectory).copyFrom(
            eq(storeDirectory),
            any(FileMetadata.class),
            any(String.class),
            eq(IOContext.DEFAULT),
            any(Runnable.class),
            any(ActionListener.class),
            eq(false)
        );
    }

    // ═══════════════════════════════════════════════════════════════
    // getExistingRemoteFilename from uploaded cache
    // ═══════════════════════════════════════════════════════════════

    public void testGetExistingRemoteFilename_fromUploadedCache() throws IOException {
        CompositeStoreDirectory storeDir = mock(CompositeStoreDirectory.class);
        FileMetadata fm = new FileMetadata("lucene", "_cached_remote.si");

        when(
            compositeRemoteDirectory.copyFrom(
                eq(storeDir),
                any(FileMetadata.class),
                any(String.class),
                any(IOContext.class),
                any(Runnable.class),
                any(ActionListener.class),
                eq(false)
            )
        ).thenAnswer(invocation -> {
            Runnable postUploadRunner = invocation.getArgument(4);
            postUploadRunner.run();
            ActionListener<Void> listener = invocation.getArgument(5);
            listener.onResponse(null);
            return true;
        });

        when(storeDir.calculateUploadChecksum(any(FileMetadata.class))).thenReturn("55555");
        when(storeDir.fileLength(any(FileMetadata.class))).thenReturn(300L);

        ActionListener<Void> listener = mock(ActionListener.class);
        directory.copyFrom(fm, storeDir, IOContext.DEFAULT, listener, false);

        String remoteFilename = directory.getExistingRemoteFilename(fm);
        assertNotNull("Should find remote filename from uploaded cache", remoteFilename);
        assertTrue("Remote filename should contain the original filename", remoteFilename.contains("_cached_remote"));
    }

    // ═══════════════════════════════════════════════════════════════
    // getSegmentsUploadedToRemoteStore after upload
    // ═══════════════════════════════════════════════════════════════

    public void testGetSegmentsUploadedToRemoteStore_afterUpload() throws IOException {
        CompositeStoreDirectory storeDir = mock(CompositeStoreDirectory.class);
        FileMetadata fm = new FileMetadata("lucene", "_seg_cache.si");

        when(
            compositeRemoteDirectory.copyFrom(
                eq(storeDir),
                any(FileMetadata.class),
                any(String.class),
                any(IOContext.class),
                any(Runnable.class),
                any(ActionListener.class),
                eq(false)
            )
        ).thenAnswer(invocation -> {
            Runnable postUploadRunner = invocation.getArgument(4);
            postUploadRunner.run();
            ActionListener<Void> listener = invocation.getArgument(5);
            listener.onResponse(null);
            return true;
        });

        when(storeDir.calculateUploadChecksum(any(FileMetadata.class))).thenReturn("77777");
        when(storeDir.fileLength(any(FileMetadata.class))).thenReturn(1024L);

        ActionListener<Void> listener = mock(ActionListener.class);
        directory.copyFrom(fm, storeDir, IOContext.DEFAULT, listener, false);

        Map<String, UploadedSegmentMetadata> cache = directory.getSegmentsUploadedToRemoteStore();
        assertFalse("Cache should not be empty after upload", cache.isEmpty());
        // The key in the returned map should be the serialized form of FileMetadata
        assertTrue("Cache should contain the uploaded file", cache.containsKey(fm.serialize()));
        assertEquals("77777", cache.get(fm.serialize()).getChecksum());
        assertEquals(1024L, cache.get(fm.serialize()).getLength());
    }

    // ═══════════════════════════════════════════════════════════════
    // copyFrom listener failure path
    // ═══════════════════════════════════════════════════════════════

    public void testCopyFrom_fileMetadata_uploadException_callsListenerOnFailure() throws IOException {
        CompositeStoreDirectory storeDir = mock(CompositeStoreDirectory.class);
        FileMetadata fm = new FileMetadata("lucene", "_fail_upload.si");

        when(
            compositeRemoteDirectory.copyFrom(
                eq(storeDir),
                any(FileMetadata.class),
                any(String.class),
                any(IOContext.class),
                any(Runnable.class),
                any(ActionListener.class),
                eq(false)
            )
        ).thenThrow(new RuntimeException("Upload failed"));

        ActionListener<Void> listener = mock(ActionListener.class);
        directory.copyFrom(fm, storeDir, IOContext.DEFAULT, listener, false);

        verify(listener).onFailure(any());
    }

    // ═══════════════════════════════════════════════════════════════
    // delete - partial failure
    // ═══════════════════════════════════════════════════════════════

    public void testDelete_metadataDirectoryFails() throws IOException {
        org.mockito.Mockito.doThrow(new IOException("metadata delete failed")).when(remoteMetadataDirectory).delete();

        boolean result = directory.delete();
        assertFalse(result);
    }

    public void testDelete_lockManagerFails() throws IOException {
        org.mockito.Mockito.doThrow(new IOException("lock delete failed")).when(mdLockManager).delete();

        boolean result = directory.delete();
        assertFalse(result);
    }

    // ═══════════════════════════════════════════════════════════════
    // unmark - empty set is no-op
    // ═══════════════════════════════════════════════════════════════

    public void testUnmarkMergedSegmentsPendingDownload_emptySet() throws IOException {
        Map<FileMetadata, String> pendingSegments = new ConcurrentHashMap<>();
        FileMetadata fm = new FileMetadata("lucene", "_stay.si");
        pendingSegments.put(fm, "_stay.si__uuid");

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.METADATA_PREFIX,
                CompositeRemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH
            )
        ).thenReturn(Collections.emptyList());

        CompositeRemoteSegmentStoreDirectory dirWithPending = new CompositeRemoteSegmentStoreDirectory(
            compositeRemoteDirectory,
            remoteMetadataDirectory,
            mdLockManager,
            threadPool,
            shardId,
            pendingSegments
        );

        // Unmark with empty set - nothing should change
        dirWithPending.unmarkMergedSegmentsPendingDownload(Set.of());
        assertTrue("File should still be pending", dirWithPending.isMergedSegmentPendingDownload(fm));
    }

    public void testUnmarkMergedSegmentsPendingDownload_allFiles() throws IOException {
        Map<FileMetadata, String> pendingSegments = new ConcurrentHashMap<>();
        FileMetadata fm1 = new FileMetadata("lucene", "_a.si");
        FileMetadata fm2 = new FileMetadata("lucene", "_b.si");
        pendingSegments.put(fm1, "_a.si__uuid");
        pendingSegments.put(fm2, "_b.si__uuid");

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.METADATA_PREFIX,
                CompositeRemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH
            )
        ).thenReturn(Collections.emptyList());

        CompositeRemoteSegmentStoreDirectory dirWithPending = new CompositeRemoteSegmentStoreDirectory(
            compositeRemoteDirectory,
            remoteMetadataDirectory,
            mdLockManager,
            threadPool,
            shardId,
            pendingSegments
        );

        // Unmark all files
        dirWithPending.unmarkMergedSegmentsPendingDownload(Set.of("_a.si", "_b.si"));
        assertFalse(dirWithPending.isMergedSegmentPendingDownload(fm1));
        assertFalse(dirWithPending.isMergedSegmentPendingDownload(fm2));
    }

    // ═══════════════════════════════════════════════════════════════
    // getMetadataFilesToFilterActiveSegments - mixed locked/unlocked
    // ═══════════════════════════════════════════════════════════════

    public void testGetMetadataFilesForActiveSegments_mixedLockedUnlocked() {
        List<String> sortedMdFiles = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            sortedMdFiles.add("metadata__" + i);
        }
        // Lock files at indices 5, 6, 7, 8
        Set<String> lockedFiles = new HashSet<>();
        lockedFiles.add("metadata__5");
        lockedFiles.add("metadata__6");
        lockedFiles.add("metadata__7");
        lockedFiles.add("metadata__8");

        int lastNToKeep = 3;
        Set<String> result = directory.getMetadataFilesToFilterActiveSegments(lastNToKeep, sortedMdFiles, lockedFiles);

        // Should include boundary files for active segment filtering
        assertNotNull(result);
        // metadata__2 should be included as the boundary for lastNToKeep=3
        assertTrue("Should include boundary metadata file", result.contains("metadata__2"));
    }

    public void testGetMetadataFilesForActiveSegments_emptyList() {
        List<String> sortedMdFiles = new ArrayList<>();
        Set<String> lockedFiles = new HashSet<>();

        Set<String> result = directory.getMetadataFilesToFilterActiveSegments(0, sortedMdFiles, lockedFiles);
        assertTrue("Empty file list should produce empty result", result.isEmpty());
    }

    public void testGetMetadataFilesForActiveSegments_keepAllFiles() {
        List<String> sortedMdFiles = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            sortedMdFiles.add("metadata__" + i);
        }
        Set<String> lockedFiles = new HashSet<>();

        // Keep all 5 files, nothing to delete
        Set<String> result = directory.getMetadataFilesToFilterActiveSegments(5, sortedMdFiles, lockedFiles);
        assertTrue("Keeping all files should produce empty result", result.isEmpty());
    }

    // ═══════════════════════════════════════════════════════════════
    // METADATA_FILES_TO_FETCH constant
    // ═══════════════════════════════════════════════════════════════

    public void testMetadataFilesToFetchConstant() {
        assertEquals(10, CompositeRemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH);
    }

    // ═══════════════════════════════════════════════════════════════
    // Multiple uploads to same file (cache overwrite)
    // ═══════════════════════════════════════════════════════════════

    public void testMultipleUploads_cacheOverwrite() throws IOException {
        CompositeStoreDirectory storeDir = mock(CompositeStoreDirectory.class);
        FileMetadata fm = new FileMetadata("lucene", "_overwrite.si");

        when(
            compositeRemoteDirectory.copyFrom(
                eq(storeDir),
                any(FileMetadata.class),
                any(String.class),
                any(IOContext.class),
                any(Runnable.class),
                any(ActionListener.class),
                eq(false)
            )
        ).thenAnswer(invocation -> {
            Runnable postUploadRunner = invocation.getArgument(4);
            postUploadRunner.run();
            ActionListener<Void> listener = invocation.getArgument(5);
            listener.onResponse(null);
            return true;
        });

        // First upload with checksum "11111"
        when(storeDir.calculateUploadChecksum(any(FileMetadata.class))).thenReturn("11111");
        when(storeDir.fileLength(any(FileMetadata.class))).thenReturn(100L);

        ActionListener<Void> listener1 = mock(ActionListener.class);
        directory.copyFrom(fm, storeDir, IOContext.DEFAULT, listener1, false);

        assertTrue(directory.containsFile(fm, "11111"));

        // Second upload with checksum "22222" (e.g., file was modified)
        when(storeDir.calculateUploadChecksum(any(FileMetadata.class))).thenReturn("22222");
        when(storeDir.fileLength(any(FileMetadata.class))).thenReturn(200L);

        ActionListener<Void> listener2 = mock(ActionListener.class);
        directory.copyFrom(fm, storeDir, IOContext.DEFAULT, listener2, false);

        // Old checksum should no longer match
        assertFalse(directory.containsFile(fm, "11111"));
        // New checksum should match
        assertTrue(directory.containsFile(fm, "22222"));
    }

    // ═══════════════════════════════════════════════════════════════
    // Non-lucene format files in pending download
    // ═══════════════════════════════════════════════════════════════

    public void testPendingDownload_nonLuceneFormat() throws IOException {
        Map<FileMetadata, String> pendingSegments = new ConcurrentHashMap<>();
        FileMetadata fm = new FileMetadata("parquet", "_data.parquet");
        pendingSegments.put(fm, "_data.parquet__remote_uuid");

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                MetadataFilenameUtils.METADATA_PREFIX,
                CompositeRemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH
            )
        ).thenReturn(Collections.emptyList());

        CompositeRemoteSegmentStoreDirectory dirWithPending = new CompositeRemoteSegmentStoreDirectory(
            compositeRemoteDirectory,
            remoteMetadataDirectory,
            mdLockManager,
            threadPool,
            shardId,
            pendingSegments
        );

        assertTrue(dirWithPending.isMergedSegmentPendingDownload(fm));
        assertEquals("_data.parquet__remote_uuid", dirWithPending.getExistingRemoteFilename(fm));

        // Different format same file name should not match
        FileMetadata fmDiffFormat = new FileMetadata("arrow", "_data.parquet");
        assertFalse(dirWithPending.isMergedSegmentPendingDownload(fmDiffFormat));
    }

    // ═══════════════════════════════════════════════════════════════
    // removeFileFromSegmentsUploadedToRemoteStore - non-lucene format
    // ═══════════════════════════════════════════════════════════════

    public void testRemoveFileFromSegmentsUploadedToRemoteStore_nonLucene() throws IOException {
        CompositeStoreDirectory storeDir = mock(CompositeStoreDirectory.class);
        FileMetadata fm = new FileMetadata("parquet", "_remove_parquet.parquet");

        when(
            compositeRemoteDirectory.copyFrom(
                eq(storeDir),
                any(FileMetadata.class),
                any(String.class),
                any(IOContext.class),
                any(Runnable.class),
                any(ActionListener.class),
                eq(false)
            )
        ).thenAnswer(invocation -> {
            Runnable postUploadRunner = invocation.getArgument(4);
            postUploadRunner.run();
            ActionListener<Void> listener = invocation.getArgument(5);
            listener.onResponse(null);
            return true;
        });

        when(storeDir.calculateUploadChecksum(any(FileMetadata.class))).thenReturn("88888");
        when(storeDir.fileLength(any(FileMetadata.class))).thenReturn(500L);

        ActionListener<Void> listener = mock(ActionListener.class);
        directory.copyFrom(fm, storeDir, IOContext.DEFAULT, listener, false);

        assertTrue(directory.containsFile(fm, "88888"));

        UploadedSegmentMetadata metadata = new UploadedSegmentMetadata(
            "_remove_parquet.parquet",
            "_remove_parquet.parquet__uuid",
            "88888",
            500,
            "parquet"
        );
        directory.removeFileFromSegmentsUploadedToRemoteStore(metadata);

        assertFalse(directory.containsFile(fm, "88888"));
    }
}
