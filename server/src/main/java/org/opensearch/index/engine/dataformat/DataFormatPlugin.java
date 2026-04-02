/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.checksum.ChecksumHandler;

import java.util.Map;

/**
 * Plugin interface for providing custom data format implementations.
 * Plugins implement this to register their data format (e.g., Parquet, Lucene)
 * with the DataFormatRegistry during node bootstrap.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DataFormatPlugin {

    /**
     * Returns the data format with its declared capabilities and priority.
     *
     * @return the data format descriptor
     */
    DataFormat getDataFormat();

    /**
     * Creates the indexing engine for the data format. This should be instantiated per shard.
     *
     * @param mapperService the mapper service for field mapping resolution
     * @param shardPath the shard path for file storage
     * @param indexSettings the index settings
     * @return the indexing execution engine instance
     */
    IndexingExecutionEngine<?, ?> indexingEngine(MapperService mapperService, ShardPath shardPath, IndexSettings indexSettings);

    /**
     * Returns a map of format name to checksum handler for all formats this plugin supports.
     * <p>
     * Most plugins return a single-entry map for their own format. Composite plugins
     * return entries for each format they orchestrate.
     *
     * @return map of format name to checksum handler
     */
    Map<String, ChecksumHandler> checksumHandlers();
}
