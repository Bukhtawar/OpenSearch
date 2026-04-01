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
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.Locale;

/**
 * Default implementation of DataFormatAwareStoreDirectoryFactory that provides
 * plugin-based format discovery and fallback behavior.
 *
 * @opensearch.experimental
 */
@ExperimentalApi()
public class DefaultDataFormatAwareStoreDirectoryFactory implements DataFormatAwareStoreDirectoryFactory {

    private static final Logger logger = LogManager.getLogger(DefaultDataFormatAwareStoreDirectoryFactory.class);

    /**
     * Creates a new DataFormatAwareStoreDirectory with plugin-based format discovery.
     *
     * @param indexSettings  the shard's index settings
     * @param shardId        the shard identifier
     * @param shardPath      the path the shard is using
     * @return a new DataFormatAwareStoreDirectory instance
     * @throws IOException if directory creation fails
     */
    @Override
    public DataFormatAwareStoreDirectory newDataFormatAwareStoreDirectory(IndexSettings indexSettings, ShardId shardId, ShardPath shardPath, DataFormatRegistry dataFormatRegistry)
        throws IOException {

        if (logger.isDebugEnabled()) {
            logger.debug(
                "Creating DataFormatAwareStoreDirectory for shard: {} at path: {}",
                shardPath.getShardId(),
                shardPath.getDataPath()
            );
        }

        try {
            // Create FSDirectory on the shard's index/ directory as the delegate
            FSDirectory delegate = FSDirectory.open(shardPath.resolveIndex());

            DataFormatAwareStoreDirectory directory = new DataFormatAwareStoreDirectory(delegate, shardPath, dataFormatRegistry);

            if (logger.isDebugEnabled()) {
                logger.debug(
                    "Successfully created DataFormatAwareStoreDirectory for shard: {} with registered formats: {}",
                    shardPath.getShardId(),
                    dataFormatRegistry.getRegisteredFormats()
                );
            }

            return directory;

        } catch (Exception e) {
            logger.error(
                () -> new org.apache.logging.log4j.message.ParameterizedMessage(
                    "Failed to create DataFormatAwareStoreDirectory for shard: {}",
                    shardPath.getShardId()
                ),
                e
            );
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Failed to create DataFormatAwareStoreDirectory for shard %s: %s",
                    shardPath.getShardId(),
                    e.getMessage()
                ),
                e
            );
        }
    }
}
