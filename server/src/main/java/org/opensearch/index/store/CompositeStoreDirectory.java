/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.logging.Loggers;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

/**
 * Format-aware composite directory that uses {@link SubdirectoryAwareDirectory} via composition.
 *
 * <p>This directory adds data format awareness on top of the subdirectory path routing
 * provided by {@link SubdirectoryAwareDirectory}. It understands that files in different
 * subdirectories may belong to different data formats (Lucene, Parquet, Arrow, etc.) and
 * provides format-specific operations, most notably checksum calculation.</p>
 *
 * <p><strong>Delegated to SubdirectoryAwareDirectory:</strong></p>
 * <ul>
 *   <li>Path routing: plain filenames → index/, prefixed filenames → subdirectories</li>
 *   <li>File operations: openInput, createOutput, deleteFile, fileLength, listAll, rename, sync</li>
 * </ul>
 *
 * <p><strong>Added by CompositeStoreDirectory:</strong></p>
 * <ul>
 *   <li>FileMetadata support: parse file identifier strings into FileMetadata objects</li>
 *   <li>Format-aware checksum: Lucene files → CodecUtil footer, others → CRC32 full-file</li>
 *   <li>Dual API: callers can use String or FileMetadata for all operations</li>
 * </ul>
 *
 * <p><strong>File naming convention:</strong></p>
 * <pre>
 *   Lucene:   "_0.cfs"                → stored in &lt;shard&gt;/index/_0.cfs
 *   Parquet:  "parquet/_0_1.parquet"  → stored in &lt;shard&gt;/parquet/_0_1.parquet
 *   Arrow:    "arrow/_0_1.arrow"      → stored in &lt;shard&gt;/arrow/_0_1.arrow
 * </pre>
 *
 * <p><strong>Checksum strategy:</strong></p>
 * <ul>
 *   <li>Lucene/index files: {@code CodecUtil.retrieveChecksum()} — reads checksum from codec footer (fast, O(1))</li>
 *   <li>Non-Lucene files: Full-file CRC32 scan — computes CRC32 over all bytes (generic, O(n))</li>
 * </ul>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class CompositeStoreDirectory extends Store.StoreDirectory {

    private static final Logger logger = LogManager.getLogger(CompositeStoreDirectory.class);

    private static final String DEFAULT_FORMAT = "lucene";

    private static final Set<String> INDEX_DIRECTORY_FORMATS = Set.of("lucene", "metadata");

    private static final int CHECKSUM_BUFFER_SIZE = 8192;

    private final SubdirectoryAwareDirectory subdirectoryAwareDirectory;
    private final ShardPath shardPath;

    /**
     * Constructs a CompositeStoreDirectory.
     *
     * @param delegate          the underlying FSDirectory (typically for &lt;shard&gt;/index/)
     * @param shardPath set of non-Lucene format names discovered from plugins
     *                          (e.g., {"parquet", "arrow"}). These formats use generic CRC32 checksums.
     */
    public CompositeStoreDirectory(Directory delegate, ShardPath shardPath) {
        super(null, Loggers.getLogger("index.store.deletes", shardPath.getShardId()));
        this.shardPath = shardPath;
        this.subdirectoryAwareDirectory = new SubdirectoryAwareDirectory(delegate, shardPath);

        logger.debug("Created CompositeStoreDirectory for shard {}", shardPath.getShardId());
    }

    private String resolveFileName(String fileName) {
        if (fileName.contains(FileMetadata.DELIMITER)) {
            FileMetadata fm = new FileMetadata(fileName);
            fileName = toFileIdentifier(fm);
        }
        return fileName;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return subdirectoryAwareDirectory.openInput(resolveFileName(name), context);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return subdirectoryAwareDirectory.createOutput(resolveFileName(name), context);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        subdirectoryAwareDirectory.deleteFile(resolveFileName(name));
    }

    @Override
    public long fileLength(String name) throws IOException {
        return subdirectoryAwareDirectory.fileLength(resolveFileName(name));
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        subdirectoryAwareDirectory.sync(names.stream().map(this::resolveFileName).collect(Collectors.toList()));
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        subdirectoryAwareDirectory.rename(resolveFileName(source), resolveFileName(dest));
    }

    @Override
    public String[] listAll() throws IOException {
        return subdirectoryAwareDirectory.listAll();
    }

    // ═══════════════════════════════════════════════════════════════
    // FileMetadata parsing and conversion
    // ═══════════════════════════════════════════════════════════════

    /**
     * Parses a file identifier string into a {@link FileMetadata} object.
     *
     * @param fileIdentifier the file path string (with optional format prefix separated by '/')
     * @return FileMetadata with parsed dataFormat and filename
     */
    public FileMetadata toFileMetadata(String fileIdentifier) {
        int slash = fileIdentifier.indexOf('/');
        if (slash >= 0) {
            String format = fileIdentifier.substring(0, slash);
            String file = fileIdentifier.substring(slash + 1);
            return new FileMetadata(format, file);
        }
        return new FileMetadata(DEFAULT_FORMAT, fileIdentifier);
    }

    /**
     * Converts a {@link FileMetadata} object back to a file identifier string.
     *
     * @param fm the FileMetadata to convert
     * @return file identifier string suitable for Directory operations
     */
    public String toFileIdentifier(FileMetadata fm) {
        String format = fm.dataFormat();
        if (isDefaultFormat(format)) {
            return fm.file();
        }
        return format + "/" + fm.file();
    }

    // ═══════════════════════════════════════════════════════════════
    // Format-Aware Checksum Calculation
    // ═══════════════════════════════════════════════════════════════

    public long calculateChecksum(String name) throws IOException {
        FileMetadata fm = toFileMetadata(name);
        return calculateChecksum(fm);
    }

    public long calculateChecksum(FileMetadata fm) throws IOException {
        String fileIdentifier = toFileIdentifier(fm);
        if (isDefaultFormat(fm.dataFormat())) {
            return calculateDefaultChecksum(fileIdentifier);
        } else {
            return calculateGenericChecksum(fileIdentifier);
        }
    }

    public String calculateUploadChecksum(FileMetadata fm) throws IOException {
        return Long.toString(calculateChecksum(fm));
    }

    // ═══════════════════════════════════════════════════════════════
    // FileMetadata-Based Convenience Operations
    // ═══════════════════════════════════════════════════════════════

    public long fileLength(FileMetadata fm) throws IOException {
        return fileLength(toFileIdentifier(fm));
    }

    public void deleteFile(FileMetadata fm) throws IOException {
        deleteFile(toFileIdentifier(fm));
    }

    public IndexInput openInput(FileMetadata fm, IOContext context) throws IOException {
        return openInput(toFileIdentifier(fm), context);
    }

    public IndexOutput createOutput(FileMetadata fm, IOContext context) throws IOException {
        return createOutput(toFileIdentifier(fm), context);
    }

    public void copyFrom(FileMetadata fm, Directory source, IOContext context) throws IOException {
        String fileIdentifier = toFileIdentifier(fm);
        try (IndexInput input = source.openInput(fm.serialize(), context); IndexOutput output = createOutput(fileIdentifier, context)) {
            output.copyBytes(input, input.length());
        }
    }

    public void rename(FileMetadata source, FileMetadata dest) throws IOException {
        if (source.dataFormat().equals(dest.dataFormat()) == false) {
            throw new IllegalArgumentException("Cannot rename across formats: " + source.dataFormat() + " -> " + dest.dataFormat());
        }
        rename(toFileIdentifier(source), toFileIdentifier(dest));
    }

    public FileMetadata[] listFileMetadata() throws IOException {
        String[] allFiles = listAll();
        FileMetadata[] result = new FileMetadata[allFiles.length];
        for (int i = 0; i < allFiles.length; i++) {
            result[i] = toFileMetadata(allFiles[i]);
        }
        return result;
    }

    public long getChecksumOfLocalFile(FileMetadata fm) throws IOException {
        return calculateChecksum(fm);
    }

    public String getDataFormat(String fileIdentifier) {
        return toFileMetadata(fileIdentifier).dataFormat();
    }

    public ShardPath getShardPath() {
        return shardPath;
    }

    // ═══════════════════════════════════════════════════════════════
    // Private Helpers
    // ═══════════════════════════════════════════════════════════════

    private boolean isDefaultFormat(String format) {
        return format == null || format.isEmpty() || INDEX_DIRECTORY_FORMATS.contains(format.toLowerCase());
    }

    private long calculateDefaultChecksum(String fileIdentifier) throws IOException {
        try (IndexInput input = openInput(fileIdentifier, IOContext.READONCE)) {
            return CodecUtil.retrieveChecksum(input);
        }
    }

    // TODO: delegate checksum calculation to the respective data format plugin for efficiency
    private long calculateGenericChecksum(String fileIdentifier) throws IOException {
        try (IndexInput input = openInput(fileIdentifier, IOContext.READONCE)) {
            CRC32 crc32 = new CRC32();
            byte[] buffer = new byte[CHECKSUM_BUFFER_SIZE];
            long remaining = input.length();

            while (remaining > 0) {
                int toRead = (int) Math.min(buffer.length, remaining);
                input.readBytes(buffer, 0, toRead);
                crc32.update(buffer, 0, toRead);
                remaining -= toRead;
            }

            return crc32.getValue();
        }
    }
}
