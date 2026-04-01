/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;

/**
 * Factory interface for creating DataFormatAwareStoreDirectory instances.
 * This interface follows the existing IndexStorePlugin pattern to provide
 * a centralized way to create composite directories with format discovery.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
@FunctionalInterface
public interface DataFormatAwareStoreDirectoryFactory {

    /**
     * Creates a new DataFormatAwareStoreDirectory per shard with automatic format discovery.
     * <p>
     * The factory will:
     * - Use PluginsService to discover available DataFormat plugins
     * - Create format-specific directories for each discovered format
     * - Provide fallback behavior if no plugins are found
     * - Handle errors gracefully with proper logging
     *
     * @param indexSettings  the shard's index settings containing configuration
     * @param shardId
     * @param shardPath      the path the shard is using for file storage
     * @return a new DataFormatAwareStoreDirectory instance supporting all discovered formats
     * @throws IOException if directory creation fails or resources cannot be allocated
     */
    DataFormatAwareStoreDirectory newDataFormatAwareStoreDirectory(IndexSettings indexSettings, ShardId shardId, ShardPath shardPath, DataFormatRegistry dataFormatRegistry)
        throws IOException;
}
