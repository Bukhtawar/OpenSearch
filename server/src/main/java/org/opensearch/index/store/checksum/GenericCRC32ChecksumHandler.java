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
import java.util.zip.CRC32;

/**
 * Generic checksum handler that computes CRC32 over the entire file contents.
 *
 * <p>This is the default/fallback handler for non-Lucene formats (Parquet, Arrow, etc.)
 * that do not embed a Lucene codec footer. It reads the entire file — O(n) complexity.</p>
 *
 * <p>Can be instantiated with a specific format name (e.g., "parquet") or with the
 * default name "generic".</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class GenericCRC32ChecksumHandler implements ChecksumHandler {

    private static final String DEFAULT_FORMAT_NAME = "generic";
    private static final int BUFFER_SIZE = 8192;

    private final String formatName;

    /**
     * Creates a handler with the default format name "generic".
     */
    public GenericCRC32ChecksumHandler() {
        this(DEFAULT_FORMAT_NAME);
    }

    /**
     * Creates a handler for a specific named format.
     *
     * @param formatName the format name this handler is registered under
     */
    public GenericCRC32ChecksumHandler(String formatName) {
        this.formatName = formatName;
    }

    @Override
    public String getFormatName() {
        return formatName;
    }

    // Note: This reads the entire input sequentially which is slow for large files
    // and may not work for all use cases.
    @Override
    public long calculateChecksum(IndexInput input) throws IOException {
        CRC32 crc32 = new CRC32();
        byte[] buffer = new byte[BUFFER_SIZE];
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
