/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.store.FormatChecksumStrategy;

import java.util.function.Supplier;

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
    private final Supplier<FormatChecksumStrategy> checksumStrategySupplier;

    /**
     * Creates a new DataFormatDescriptor.
     *
     * @param formatName              the format name (e.g., "parquet")
     * @param checksumStrategySupplier supplier for the checksum strategy for this format
     */
    public DataFormatDescriptor(String formatName, Supplier<FormatChecksumStrategy> checksumStrategySupplier) {
        this.formatName = formatName;
        this.checksumStrategySupplier = checksumStrategySupplier;
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
     * Returns a checksum strategy for this format.
     *
     * @return the checksum strategy
     */
    public FormatChecksumStrategy getChecksumHandler() {
        return checksumStrategySupplier.get();
    }
}
