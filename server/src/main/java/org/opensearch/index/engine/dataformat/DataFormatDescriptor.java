/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.store.checksum.ChecksumHandler;

/**
 * Describes the runtime capabilities of a data format, including its checksum handler
 * and format name. Provided by {@link DataFormatPlugin} implementations and consumed
 * by DataFormatAwareStoreDirectory and DataFormatAwareRemoteDirectory.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatDescriptor {

    private final String formatName;
    private final ChecksumHandler checksumHandler;

    /**
     * Creates a new DataFormatDescriptor.
     *
     * @param formatName      the format name (e.g., "parquet")
     * @param checksumHandler the checksum handler for this format
     */
    public DataFormatDescriptor(String formatName, ChecksumHandler checksumHandler) {
        this.formatName = formatName;
        this.checksumHandler = checksumHandler;
    }

    /**
     * Returns the format name.
     *
     * @return the format name
     */
    public String getFormatName() {
        return formatName;
    }

    /**
     * Returns the checksum handler for this format.
     *
     * @return the checksum handler
     */
    public ChecksumHandler getChecksumHandler() {
        return checksumHandler;
    }
}
