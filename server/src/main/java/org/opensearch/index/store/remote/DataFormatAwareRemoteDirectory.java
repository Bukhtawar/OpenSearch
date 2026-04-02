/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.ExceptionsHelper;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.exception.CorruptFileException;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeInputStream;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.index.store.DataFormatAwareStoreDirectory;
import org.opensearch.index.store.FileMetadata;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteIndexInput;
import org.opensearch.index.store.RemoteIndexOutput;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/**
 * DataFormatAwareRemoteDirectory extends RemoteDirectory with format-aware blob routing.
 *
 * <p>This directory routes file operations to format-specific BlobContainers based on the
 * data format encoded in the filename (using the "filename:::format" convention from FileMetadata).
 *
 * <p>Key design decisions:
 * <ul>
 *   <li>"lucene" format (or no format) → inherited blobContainer at baseBlobPath (same as RemoteDirectory)</li>
 *   <li>Non-lucene formats (e.g., "parquet") → baseBlobPath/formatName/ sub-path</li>
 *   <li>String-based overrides parse "filename:::format" to route to the correct BlobContainer</li>
 *   <li>UploadedSegmentMetadata-based overrides extract format from originalFilename for delete/open</li>
 * </ul>
 *
 * @opensearch.api
 */
@InternalApi
public class DataFormatAwareRemoteDirectory extends RemoteDirectory {

    private static final String DEFAULT_FORMAT = "lucene";

    /**
     * Formats that should route to the base blobContainer (same path as RemoteDirectory).
     * Mirrors DataFormatAwareStoreDirectory.INDEX_DIRECTORY_FORMATS.
     */
    private static final java.util.Set<String> BASE_PATH_FORMATS = java.util.Set.of("lucene", "LUCENE", "metadata");

    private final UnaryOperator<OffsetRangeInputStream> uploadRateLimiter;
    private final UnaryOperator<OffsetRangeInputStream> lowPriorityUploadRateLimiter;
    private final DownloadRateLimiterProvider downloadRateLimiterProvider;

    private final Map<String, BlobContainer> formatBlobContainers;
    private final BlobStore blobStore;
    private final BlobPath baseBlobPath;
    private final Logger logger;

    /**
     * Format lookup cache: maps blob keys (e.g., "_0.pqt__UUID") to their data format (e.g., "parquet").
     * Populated by RemoteSegmentStoreDirectory via registerBlobFormat/replaceBlobFormatCache at well-defined
     * mutation points (init, postUpload, deleteFile, markMergedSegmentsPendingDownload, etc.).
     *
     * <p>Volatile for atomic reference swap in {@link #replaceBlobFormatCache(Map)}.
     */
    private volatile ConcurrentHashMap<String, String> blobFormatCache = new ConcurrentHashMap<>();

    /**
     * Full constructor with all rate limiter parameters.
     */
    public DataFormatAwareRemoteDirectory(
        BlobStore blobStore,
        BlobPath baseBlobPath,
        UnaryOperator<OffsetRangeInputStream> uploadRateLimiter,
        UnaryOperator<OffsetRangeInputStream> lowPriorityUploadRateLimiter,
        UnaryOperator<InputStream> downloadRateLimiter,
        UnaryOperator<InputStream> lowPriorityDownloadRateLimiter,
        Map<String, String> pendingDownloadMergedSegments,
        Logger logger,
        PluginsService pluginsService
    ) {
        super(
            blobStore.blobContainer(baseBlobPath),
            uploadRateLimiter,
            lowPriorityUploadRateLimiter,
            downloadRateLimiter,
            lowPriorityDownloadRateLimiter,
            pendingDownloadMergedSegments
        );
        this.formatBlobContainers = new ConcurrentHashMap<>();
        this.blobStore = blobStore;
        this.baseBlobPath = baseBlobPath;
        this.uploadRateLimiter = uploadRateLimiter;
        this.lowPriorityUploadRateLimiter = lowPriorityUploadRateLimiter;
        this.downloadRateLimiterProvider = new DownloadRateLimiterProvider(downloadRateLimiter, lowPriorityDownloadRateLimiter);
        this.logger = logger;

        // Initialize format-specific BlobContainers from plugins
        // TODO: Initialize blobstore once plugin classes are created.
        // When DataSourcePlugin integration is added, iterate over plugins with explicit null checks:
        // List<DataSourcePlugin> plugins = pluginsService.filterPlugins(DataSourcePlugin.class);
        // for (DataSourcePlugin plugin : plugins) {
        // if (plugin != null && plugin.getDataFormat() != null) {
        // BlobContainer container = plugin.createBlobContainer(blobStore, baseBlobPath);
        // if (container != null) formatBlobContainers.put(plugin.getDataFormat().name(), container);
        // }
        // }

        logger.debug("Created DataFormatAwareRemoteDirectory with {} format BlobContainers", formatBlobContainers.size());
    }

    // ═══════════════════════════════════════════════════════════════
    // Format registration — overrides from RemoteDirectory
    // Maintains format lookup cache for plain blob key routing
    // ═══════════════════════════════════════════════════════════════

    @Override
    public void registerBlobFormat(String blobKey, String format) {
        if (blobKey != null && format != null) {
            blobFormatCache.put(blobKey, format);
        }
    }

    @Override
    public void unregisterBlobFormat(String blobKey) {
        if (blobKey != null) {
            blobFormatCache.remove(blobKey);
        }
    }

    @Override
    public void replaceBlobFormatCache(Map<String, String> blobKeyToFormat) {
        // Atomic reference swap — avoids clear+put race condition.
        // Safe because init() is called during shard startup before any concurrent uploads.
        this.blobFormatCache = new ConcurrentHashMap<>(blobKeyToFormat != null ? blobKeyToFormat : Map.of());
    }

    /**
     * Resolve the data format for a given name/blob key.
     *
     * <p>Priority:
     * <ol>
     *   <li>If name contains ":::format" delimiter → parse it (e.g., copyFrom paths)</li>
     *   <li>If name is in blobFormatCache → use cached format (e.g., plain blob keys from RSSD)</li>
     *   <li>Default → "lucene"</li>
     * </ol>
     *
     * @param name the filename or blob key to resolve format for
     * @return the resolved data format name
     */
    private String resolveFormat(String name) {
        // 1. Inline format info (e.g., "_0.pqt:::parquet")
        if (name.contains(FileMetadata.DELIMITER)) {
            return new FileMetadata(name).dataFormat();
        }
        // 2. Cached lookup (e.g., "_0.pqt__UUID" → "parquet")
        String cached = blobFormatCache.get(name);
        if (cached != null) {
            return cached;
        }
        // 3. Warn if this looks like a UUID-suffixed blob key — it should have been in cache
        if (name.contains(RemoteSegmentStoreDirectory.SEGMENT_NAME_UUID_SEPARATOR)) {
            logger.warn("Format cache miss for blob key [{}], defaulting to lucene", name);
        }
        return DEFAULT_FORMAT;
    }

    // ═══════════════════════════════════════════════════════════════
    // Format Routing — the core routing logic
    // ═══════════════════════════════════════════════════════════════

    /**
     * Get BlobContainer for a specific data format.
     *
     * <p>Critical invariant: "lucene" format (and null/empty) returns the inherited
     * blobContainer at baseBlobPath — the exact same location a plain RemoteDirectory would use.
     * This ensures backward compatibility with non-optimized indices.
     *
     * <p>Non-lucene formats are routed to baseBlobPath/formatName/ sub-paths.
     *
     * @param format the data format name (e.g., "lucene", "parquet")
     * @return BlobContainer for the format
     */
    public BlobContainer getBlobContainerForFormat(String format) {
        // "lucene", "LUCENE", "metadata" and null/empty go to the default blobContainer (baseBlobPath directly)
        if (format == null || format.isEmpty() || BASE_PATH_FORMATS.contains(format)) {
            return blobContainer; // inherited from RemoteDirectory — points to baseBlobPath
        }
        // Check plugin-registered containers first, then lazily create
        return formatBlobContainers.computeIfAbsent(format, f -> {
            BlobPath formatPath = baseBlobPath.add(f.toLowerCase(java.util.Locale.ROOT));
            BlobContainer container = blobStore.blobContainer(formatPath);
            logger.debug("Created new BlobContainer for format {} at path: {}", f, formatPath);
            return container;
        });
    }

    // ═══════════════════════════════════════════════════════════════
    // Aggregated operations across all format containers
    // ═══════════════════════════════════════════════════════════════

    /**
     * Lists all blobs across the base container and all format-specific containers.
     * Results are sorted in UTF-16 order as required by the Directory contract.
     */
    @Override
    public String[] listAll() throws IOException {
        Set<String> allBlobs = new LinkedHashSet<>(Arrays.asList(super.listAll()));
        for (Map.Entry<String, BlobContainer> entry : formatBlobContainers.entrySet()) {
            allBlobs.addAll(entry.getValue().listBlobs().keySet());
        }
        String[] result = allBlobs.toArray(new String[0]);
        Arrays.sort(result);
        return result;
    }

    /**
     * Format-aware deleteFile override.
     *
     * <p>Uses {@link #resolveFormat(String)} to determine which BlobContainer to delete from.
     * For names with ":::format" suffix, parses the format inline. For plain blob keys
     * (e.g., "_0.pqt__UUID"), looks up the format from the blobFormatCache populated by RSSD.
     * Falls back to "lucene" (base container) if no format info is available.
     */
    @Override
    public void deleteFile(String name) throws IOException {
        // name is always a plain blob key (e.g., "_0.pqt__UUID") from RSSD — never contains ":::format"
        String format = resolveFormat(name);
        BlobContainer container = getBlobContainerForFormat(format);
        container.deleteBlobsIgnoringIfNotExists(Collections.singletonList(name));
    }

    /**
     * Format-aware batch delete override.
     *
     * <p>Note: This method receives plain blob names (e.g., "_0.parquet__UUID") without format info,
     * so it attempts deletion from ALL containers (base + format-specific). This is used during
     * stale segment cleanup where the caller doesn't have format information.
     */
    @Override
    public void deleteFiles(List<String> names) throws IOException {
        if (names == null || names.isEmpty()) {
            return;
        }
        // Delete from base container (handles lucene/metadata blobs)
        super.deleteFiles(names);

        // Broadcast delete to every format-specific container. This is intentionally speculative:
        // blob names are UUID-suffixed and globally unique, so at most one container holds each
        // blob.
        for (BlobContainer container : formatBlobContainers.values()) {
            container.deleteBlobsIgnoringIfNotExists(names);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // String-based overrides — called by RemoteSegmentStoreDirectory
    // These parse "filename:::format" from the src string
    // ═══════════════════════════════════════════════════════════════

    /**
     * Sync copyFrom override that properly handles format-aware local files.
     *
     * <p>When AsyncMultiStreamBlobContainer is not available (e.g., FS-based blob store in tests),
     * this fallback is used. The src string may contain ":::format" (e.g., "_0.pqt:::parquet")
     * which the source DataFormatAwareStoreDirectory handles via parseFilePath().
     *
     * <p>Routes to the format-specific BlobContainer via {@link #getBlobContainerForFormat(String)}:
     * lucene/metadata files → base blobContainer, non-lucene formats (parquet, etc.) → format-specific sub-path.
     * The download side uses {@link #openInput(RemoteSegmentStoreDirectory.UploadedSegmentMetadata, long, IOContext)}
     * which performs the same format-aware routing.
     */
    @Override
    public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        logger.debug("Sync copyFrom: src={}, dest={}", src, dest);
        FileMetadata fileMetadata = new FileMetadata(src);
        BlobContainer container = getBlobContainerForFormat(fileMetadata.dataFormat());
        // Read from local directory (DataFormatAwareStoreDirectory handles ":::format" in src)
        // Write to format-specific BlobContainer (lucene→base, parquet→parquet sub-path, etc.)
        try (IndexInput is = from.openInput(src, context); IndexOutput os = new RemoteIndexOutput(dest, container)) {
            os.copyBytes(is, is.length());
        }
    }

    /**
     * Format-aware async copyFrom override.
     *
     * <p>Parses the src string (e.g., "_0.pqt:::parquet") to determine format routing.
     * Opens local file using src as-is (DataFormatAwareStoreDirectory handles ::: parsing).
     * Uploads to the format-specific BlobContainer using remoteFileName as the blob key.
     */
    @Override
    public boolean copyFrom(
        Directory from,
        String src,
        String remoteFileName,
        IOContext context,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        boolean lowPriorityUpload,
        CryptoMetadata cryptoMetadata
    ) {
        try {
            FileMetadata fileMetadata = new FileMetadata(src);
            BlobContainer container = getBlobContainerForFormat(fileMetadata.dataFormat());

            if (container instanceof AsyncMultiStreamBlobContainer) {
                logger.debug(
                    "Format-aware upload: src={}, format={}, remoteFile={}, container={}",
                    src,
                    fileMetadata.dataFormat(),
                    remoteFileName,
                    container.path()
                );
                uploadBlob(from, src, remoteFileName, container, context, postUploadRunner, listener, lowPriorityUpload, cryptoMetadata);
                return true;
            }

            logger.warn("BlobContainer for format {} does not support async multi-stream upload", fileMetadata.dataFormat());
            return false;
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Failed format-aware upload: src={}, error={}", src, e.getMessage()), e);
            listener.onFailure(e);
            return true; // Handled (even though failed)
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // UploadedSegmentMetadata-based overrides
    // These extract format from originalFilename for routing
    // ═══════════════════════════════════════════════════════════════

    /**
     * Format-aware deleteFile using UploadedSegmentMetadata.
     *
     * <p>Parses originalFilename (e.g., "_0.pqt:::parquet") to determine which BlobContainer
     * to delete from. Uses uploadedFilename (e.g., "_0.pqt__UUID") as the actual blob key.
     */
    @Override
    public void deleteFile(RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata) throws IOException {
        FileMetadata fileMetadata = new FileMetadata(uploadedSegmentMetadata.getOriginalFilename());
        BlobContainer container = getBlobContainerForFormat(fileMetadata.dataFormat());
        container.deleteBlobsIgnoringIfNotExists(Collections.singletonList(uploadedSegmentMetadata.getUploadedFilename()));
    }

    /**
     * Format-aware openInput using UploadedSegmentMetadata.
     *
     * <p>Parses originalFilename (e.g., "_0.pqt:::parquet") to determine which BlobContainer
     * to read from. Uses uploadedFilename (e.g., "_0.pqt__UUID") as the actual blob key.
     */
    @Override
    public IndexInput openInput(RemoteSegmentStoreDirectory.UploadedSegmentMetadata metadata, long fileLength, IOContext context)
        throws IOException {
        FileMetadata fileMetadata = new FileMetadata(metadata.getOriginalFilename());
        BlobContainer container = getBlobContainerForFormat(fileMetadata.dataFormat());

        InputStream inputStream = null;
        try {
            inputStream = container.readBlob(metadata.getUploadedFilename());
            UnaryOperator<InputStream> rateLimiter = downloadRateLimiterProvider.get(metadata.getUploadedFilename());
            return new RemoteIndexInput(metadata.getUploadedFilename(), rateLimiter.apply(inputStream), fileLength);
        } catch (Exception e) {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception closeEx) {
                    e.addSuppressed(closeEx);
                }
            }
            logger.error(
                () -> new ParameterizedMessage(
                    "Exception reading blob: file={}, format={}, uploaded={}",
                    fileMetadata.file(),
                    fileMetadata.dataFormat(),
                    metadata.getUploadedFilename()
                ),
                e
            );
            throw e;
        }
    }

    /**
     * Format-aware fileLength override.
     *
     * <p>Uses {@link #resolveFormat(String)} for format resolution. For names with ":::format",
     * parses inline. For plain blob keys, looks up from blobFormatCache. Falls back to "lucene".
     */
    @Override
    public long fileLength(String name) throws IOException {
        // name is always a plain blob key (e.g., "_0.pqt__UUID") from RSSD — never contains ":::format"
        String format = resolveFormat(name);
        BlobContainer container = getBlobContainerForFormat(format);

        if (container == null) {
            throw new NoSuchFileException(String.format(java.util.Locale.ROOT, "No container for format %s, file %s", format, name));
        }

        List<BlobMetadata> metadata = container.listBlobsByPrefixInSortedOrder(name, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
        if (metadata.size() == 1 && metadata.get(0).name().equals(name)) {
            return metadata.get(0).length();
        }
        throw new NoSuchFileException(name);
    }

    // ═══════════════════════════════════════════════════════════════
    // FileMetadata-based convenience methods
    // Used by callers that have FileMetadata objects (e.g., DataFormatAwareStoreDirectory)
    // ═══════════════════════════════════════════════════════════════

    /**
     * Copy from DataFormatAwareStoreDirectory using FileMetadata for synchronous upload.
     */
    public void copyFrom(DataFormatAwareStoreDirectory from, FileMetadata src, String dest, IOContext context) throws IOException {
        boolean success = false;
        try (IndexInput is = from.openInput(src, IOContext.READONCE); IndexOutput os = createOutput(dest, src.dataFormat(), context)) {
            os.copyBytes(is, is.length());
            success = true;
        } finally {
            if (!success) {
                from.deleteFile(src);
            }
        }
    }

    /**
     * Copy from DataFormatAwareStoreDirectory using FileMetadata for async upload with format routing.
     */
    public boolean copyFrom(
        DataFormatAwareStoreDirectory from,
        FileMetadata fileMetadata,
        String remoteFileName,
        IOContext context,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        boolean lowPriorityUpload
    ) {
        try {
            BlobContainer container = getBlobContainerForFormat(fileMetadata.dataFormat());

            if (container instanceof AsyncMultiStreamBlobContainer) {
                uploadBlobFromComposite(
                    from,
                    fileMetadata,
                    remoteFileName,
                    container,
                    context,
                    postUploadRunner,
                    listener,
                    lowPriorityUpload
                );
                return true;
            }
            return false;
        } catch (Exception e) {
            listener.onFailure(e);
            return true;
        }
    }

    /**
     * Get file length using FileMetadata.
     */
    public long fileLength(FileMetadata fileMetadata) throws IOException {
        BlobContainer container = getBlobContainerForFormat(fileMetadata.dataFormat());

        if (container == null) {
            throw new NoSuchFileException(
                String.format(java.util.Locale.ROOT, "No container for format %s, file %s", fileMetadata.dataFormat(), fileMetadata.file())
            );
        }

        List<BlobMetadata> metadata = container.listBlobsByPrefixInSortedOrder(
            fileMetadata.file(),
            1,
            BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
        );
        if (metadata.size() == 1 && metadata.get(0).name().equals(fileMetadata.file())) {
            return metadata.get(0).length();
        }
        throw new NoSuchFileException(fileMetadata.file());
    }

    /**
     * Open input using FileMetadata.
     */
    public IndexInput openInput(FileMetadata fileMetadata, long fileLength, IOContext context) throws IOException {
        BlobContainer container = getBlobContainerForFormat(fileMetadata.dataFormat());

        if (container == null) {
            throw new IOException(
                String.format(java.util.Locale.ROOT, "No container for format %s, file %s", fileMetadata.dataFormat(), fileMetadata.file())
            );
        }

        InputStream inputStream = null;
        try {
            inputStream = container.readBlob(fileMetadata.file());
            UnaryOperator<InputStream> rateLimiter = downloadRateLimiterProvider.get(fileMetadata.file());
            return new RemoteIndexInput(fileMetadata.file(), rateLimiter.apply(inputStream), fileLength);
        } catch (Exception e) {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception closeEx) {
                    e.addSuppressed(closeEx);
                }
            }
            logger.error(
                () -> new ParameterizedMessage(
                    "Exception reading blob: file={}, format={}",
                    fileMetadata.file(),
                    fileMetadata.dataFormat()
                ),
                e
            );
            throw e;
        }
    }

    /**
     * Create output for a specific format.
     */
    public RemoteIndexOutput createOutput(String remoteFileName, String dataFormat, IOContext context) throws IOException {
        BlobContainer container = getBlobContainerForFormat(dataFormat);
        if (container == null) {
            throw new IOException(String.format(java.util.Locale.ROOT, "No container for format %s, file %s", dataFormat, remoteFileName));
        }
        return new RemoteIndexOutput(remoteFileName, container);
    }

    // ═══════════════════════════════════════════════════════════════
    // Lifecycle
    // ═══════════════════════════════════════════════════════════════

    @Override
    public void delete() throws IOException {
        // Delete all format-specific containers
        for (BlobContainer container : formatBlobContainers.values()) {
            container.delete();
        }
        // Also delete the base container (inherited from RemoteDirectory)
        super.delete();
        logger.debug("Deleted all containers from DataFormatAwareRemoteDirectory");
    }

    @Override
    public void close() throws IOException {
        blobFormatCache = new ConcurrentHashMap<>();
        formatBlobContainers.clear();
    }

    @Override
    public String toString() {
        return "DataFormatAwareRemoteDirectory{" + "formats=" + formatBlobContainers.keySet() + ", basePath=" + baseBlobPath + '}';
    }

    // ═══════════════════════════════════════════════════════════════
    // Private upload helpers
    // ═══════════════════════════════════════════════════════════════

    /**
     * Upload blob using String-based src. Opens local file from 'from' directory using src as-is.
     * The target BlobContainer is determined by the caller (format-aware routing already done).
     */
    private void uploadBlob(
        Directory from,
        String src,
        String remoteFileName,
        BlobContainer targetContainer,
        IOContext ioContext,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        boolean lowPriorityUpload,
        CryptoMetadata cryptoMetadata
    ) throws Exception {
        assert ioContext != IOContext.READONCE : "Remote upload will fail with IoContext.READONCE";
        long expectedChecksum;
        DataFormatAwareStoreDirectory dfasd = DataFormatAwareStoreDirectory.unwrap(from);
        if (dfasd != null) {
            expectedChecksum = dfasd.calculateChecksum(new FileMetadata(src));
        } else {
            expectedChecksum = calculateChecksumOfChecksum(from, src);
        }
        IndexInput indexInput = from.openInput(src, ioContext);
        try {
            long contentLength = indexInput.length();
            boolean remoteIntegrityEnabled = (targetContainer instanceof AsyncMultiStreamBlobContainer)
                && ((AsyncMultiStreamBlobContainer) targetContainer).remoteIntegrityCheckSupported();

            lowPriorityUpload = lowPriorityUpload || contentLength > ByteSizeUnit.GB.toBytes(15);

            RemoteTransferContainer.OffsetRangeInputStreamSupplier supplier = lowPriorityUpload
                ? (size, position) -> lowPriorityUploadRateLimiter.apply(
                    new OffsetRangeIndexInputStream(indexInput.clone(), size, position)
                )
                : (size, position) -> uploadRateLimiter.apply(new OffsetRangeIndexInputStream(indexInput.clone(), size, position));

            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                src,
                remoteFileName,
                contentLength,
                true,
                lowPriorityUpload ? WritePriority.LOW : WritePriority.NORMAL,
                supplier,
                expectedChecksum,
                remoteIntegrityEnabled
            );

            ActionListener<Void> completionListener = createCompletionListener(
                src,
                postUploadRunner,
                listener,
                remoteTransferContainer,
                indexInput
            );

            WriteContext writeContext = remoteTransferContainer.createWriteContext();
            ((AsyncMultiStreamBlobContainer) targetContainer).asyncBlobUpload(writeContext, completionListener);
        } catch (Exception e) {
            logger.warn("Exception while calling asyncBlobUpload for {}, closing IndexInput", src);
            indexInput.close();
            throw e;
        }
    }

    /**
     * Upload blob from DataFormatAwareStoreDirectory using FileMetadata.
     */
    private void uploadBlobFromComposite(
        DataFormatAwareStoreDirectory from,
        FileMetadata fileMetadata,
        String remoteFileName,
        BlobContainer targetContainer,
        IOContext ioContext,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        boolean lowPriorityUpload
    ) throws Exception {
        assert ioContext != IOContext.READONCE : "Remote upload will fail with IoContext.READONCE";
        long expectedChecksum = from.calculateChecksum(fileMetadata);
        IndexInput indexInput = from.openInput(fileMetadata, ioContext);
        try {
            long contentLength = indexInput.length();
            boolean remoteIntegrityEnabled = (targetContainer instanceof AsyncMultiStreamBlobContainer)
                && ((AsyncMultiStreamBlobContainer) targetContainer).remoteIntegrityCheckSupported();

            lowPriorityUpload = lowPriorityUpload || contentLength > ByteSizeUnit.GB.toBytes(15);

            RemoteTransferContainer.OffsetRangeInputStreamSupplier supplier = lowPriorityUpload
                ? (size, position) -> lowPriorityUploadRateLimiter.apply(
                    new OffsetRangeIndexInputStream(indexInput.clone(), size, position)
                )
                : (size, position) -> uploadRateLimiter.apply(new OffsetRangeIndexInputStream(indexInput.clone(), size, position));

            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                fileMetadata.file(),
                remoteFileName,
                contentLength,
                true,
                lowPriorityUpload ? WritePriority.LOW : WritePriority.NORMAL,
                supplier,
                expectedChecksum,
                remoteIntegrityEnabled
            );

            ActionListener<Void> completionListener = createCompletionListener(
                fileMetadata.file(),
                postUploadRunner,
                listener,
                remoteTransferContainer,
                indexInput
            );

            WriteContext writeContext = remoteTransferContainer.createWriteContext();
            ((AsyncMultiStreamBlobContainer) targetContainer).asyncBlobUpload(writeContext, completionListener);
        } catch (Exception e) {
            logger.warn("Exception while calling asyncBlobUpload for {}, closing IndexInput", fileMetadata.file());
            indexInput.close();
            throw e;
        }
    }

    /**
     * Opens a stream for reading the existing file and returns {@link RemoteIndexInput} enclosing
     * the stream.
     *
     * <p>Uses {@link #resolveFormat(String)} for format resolution: inline ":::format" → cache → default "lucene".
     *
     * @param name the name of an existing file.
     * @param fileLength file length
     * @param context desired {@link IOContext} context
     * @return the {@link RemoteIndexInput} enclosing the stream
     * @throws IOException in case of I/O error
     * @throws NoSuchFileException if the file does not exist
     */
    @Override
    public IndexInput openInput(String name, long fileLength, IOContext context) throws IOException {
        String format = resolveFormat(name);
        BlobContainer container = getBlobContainerForFormat(format);
        InputStream inputStream = null;
        try {
            inputStream = container.readBlob(name);
            UnaryOperator<InputStream> rateLimiter = downloadRateLimiterProvider.get(name);
            return new RemoteIndexInput(name, rateLimiter.apply(inputStream), fileLength);
        } catch (Exception e) {
            // In case the RemoteIndexInput creation fails, close the input stream to avoid file handler leak.
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception closeEx) {
                    e.addSuppressed(closeEx);
                }
            }
            logger.error("Exception while reading blob for file: {} format: {} path: {}", name, format, blobContainer.path());
            throw e;
        }
    }

    /**
     * Format-aware openBlockInput override.
     *
     * <p>Uses {@link #resolveFormat(String)} to determine which BlobContainer to read the block from.
     * This is critical for non-lucene files accessed via plain blob keys (e.g., from CompositeDirectory
     * warm/searchable snapshot reads through RSSD's openBlockInput).
     *
     * @param name the name of an existing file (blob key).
     * @param position block start position
     * @param length block length
     * @param fileLength total file length
     * @param context desired {@link IOContext} context
     * @return the {@link IndexInput} enclosing the block data
     * @throws IOException in case of I/O error
     * @throws NoSuchFileException if the file does not exist
     */
    @Override
    public IndexInput openBlockInput(String name, long position, long length, long fileLength, IOContext context) throws IOException {
        String format = resolveFormat(name);
        BlobContainer container = getBlobContainerForFormat(format);
        if (position < 0 || length <= 0 || (position + length > fileLength)) {
            throw new IllegalArgumentException("Invalid values of block start and size");
        }
        byte[] bytes;
        try (InputStream inputStream = container.readBlob(name, position, length)) {
            UnaryOperator<InputStream> rateLimiter = downloadRateLimiterProvider.get(name);
            bytes = rateLimiter.apply(inputStream).readAllBytes();
        } catch (Exception e) {
            logger.error("Exception while reading block for file: {} format: {} path: {}", name, format, blobContainer.path());
            throw e;
        }
        return new ByteArrayIndexInput(name, bytes);
    }

    private ActionListener<Void> createCompletionListener(
        String fileName,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        RemoteTransferContainer remoteTransferContainer,
        IndexInput indexInput
    ) {
        ActionListener<Void> completionListener = ActionListener.wrap(resp -> {
            try {
                postUploadRunner.run();
                listener.onResponse(null);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("Exception in segment postUpload for file [{}]", fileName), e);
                listener.onFailure(e);
            }
        }, ex -> {
            logger.error(() -> new ParameterizedMessage("Failed to upload blob {}", fileName), ex);
            IOException corruptIndexException = ExceptionsHelper.unwrapCorruption(ex);
            if (corruptIndexException != null) {
                listener.onFailure(corruptIndexException);
                return;
            }
            Throwable throwable = ExceptionsHelper.unwrap(ex, CorruptFileException.class);
            if (throwable != null) {
                CorruptFileException cfe = (CorruptFileException) throwable;
                listener.onFailure(new CorruptIndexException(cfe.getMessage(), cfe.getFileName()));
                return;
            }
            listener.onFailure(ex);
        });

        completionListener = ActionListener.runBefore(completionListener, () -> {
            try {
                remoteTransferContainer.close();
            } catch (Exception e) {
                logger.warn("Error closing RemoteTransferContainer", e);
            }
        });

        completionListener = ActionListener.runAfter(completionListener, () -> {
            try {
                indexInput.close();
            } catch (IOException e) {
                logger.warn("Error closing IndexInput", e);
            }
        });

        return completionListener;
    }

    // ═══════════════════════════════════════════════════════════════
    // Private helpers
    // ═══════════════════════════════════════════════════════════════

    private boolean isMergedSegment(String remoteFilename) {
        return pendingDownloadMergedSegments != null && pendingDownloadMergedSegments.containsValue(remoteFilename);
    }

    /**
     * DownloadRateLimiterProvider returns a low-priority rate limited stream if the segment
     * being downloaded is a merged segment.
     */
    private class DownloadRateLimiterProvider {
        private final UnaryOperator<InputStream> downloadRateLimiter;
        private final UnaryOperator<InputStream> lowPriorityDownloadRateLimiter;

        DownloadRateLimiterProvider(
            UnaryOperator<InputStream> downloadRateLimiter,
            UnaryOperator<InputStream> lowPriorityDownloadRateLimiter
        ) {
            this.downloadRateLimiter = downloadRateLimiter;
            this.lowPriorityDownloadRateLimiter = lowPriorityDownloadRateLimiter;
        }

        public UnaryOperator<InputStream> get(final String filename) {
            if (isMergedSegment(filename)) {
                return lowPriorityDownloadRateLimiter;
            }
            return downloadRateLimiter;
        }
    }
}
