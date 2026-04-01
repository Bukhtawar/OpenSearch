/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.checksum;

import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;

/**
 * Strategy interface for format-specific checksum calculation.
 *
 * <p>Different data formats (Lucene, Parquet, Arrow, etc.) store checksums differently.
 * Implementations of this interface encapsulate the format-specific logic for
 * extracting or computing checksums from segment files.</p>
 *
 * <p>Built-in implementations:</p>
 * <ul>
 *   <li>{@link LuceneChecksumHandler} — reads checksum from Lucene codec footer (O(1))</li>
 *   <li>{@link GenericCRC32ChecksumHandler} — computes CRC32 over full file contents (O(n))</li>
 * </ul>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public interface ChecksumHandler {

    /**
     * Returns the name of the data format this handler supports.
     * This name is used as the key in the {@link org.opensearch.index.engine.dataformat.DataFormatRegistry}.
     *
     * @return format name (e.g., "lucene", "parquet", "arrow")
     */
    String getFormatName();

    /**
     * Calculates the checksum for the given IndexInput.
     *
     * @param input the IndexInput to calculate the checksum from (caller manages lifecycle)
     * @return the checksum value as a long
     * @throws IOException if an I/O error occurs during checksum calculation
     */
    long calculateChecksum(IndexInput input) throws IOException;

    /**
     * Calculates a checksum suitable for upload verification and returns it as a string.
     *
     * <p>Default implementation converts the long checksum to a string via {@code Long.toString()}.
     * Subclasses may override for format-specific string representations.</p>
     *
     * @param input the IndexInput to calculate the checksum from
     * @return checksum as a string representation
     * @throws IOException if an I/O error occurs during checksum calculation
     */
    default String calculateUploadChecksum(IndexInput input) throws IOException {
        return Long.toString(calculateChecksum(input));
    }
}
