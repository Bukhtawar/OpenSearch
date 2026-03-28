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
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.checksum.ChecksumHandlerRegistry;

import java.io.IOException;
import java.util.Locale;

/**
 * Default implementation of CompositeStoreDirectoryFactory that provides
 * plugin-based format discovery and fallback behavior.
 *
 * @opensearch.experimental
 */
@ExperimentalApi()
public class DefaultCompositeStoreDirectoryFactory implements CompositeStoreDirectoryFactory {

    private static final Logger logger = LogManager.getLogger(DefaultCompositeStoreDirectoryFactory.class);

    /**
     * Creates a new CompositeStoreDirectory with plugin-based format discovery.
     *
     * @param indexSettings  the shard's index settings
     * @param shardId        the shard identifier
     * @param shardPath      the path the shard is using
     * @return a new CompositeStoreDirectory instance
     * @throws IOException if directory creation fails
     */
    @Override
    public CompositeStoreDirectory newCompositeStoreDirectory(IndexSettings indexSettings, ShardId shardId, ShardPath shardPath)
        throws IOException {

        if (logger.isDebugEnabled()) {
            logger.debug("Creating CompositeStoreDirectory for shard: {} at path: {}", shardPath.getShardId(), shardPath.getDataPath());
        }

        try {
            // Build ChecksumHandlerRegistry with built-in handlers
            // TODO: When DataSourcePlugin is implemented, collect plugin-provided checksum handlers here:
            // var dataSourcePlugins = pluginsService.filterPlugins(DataSourcePlugin.class);
            // for (DataSourcePlugin plugin : dataSourcePlugins) {
            // checksumRegistry.registerHandler(plugin.getChecksumHandler());
            // }
            ChecksumHandlerRegistry checksumRegistry = new ChecksumHandlerRegistry();

            // Create FSDirectory on the shard's index/ directory as the delegate
            FSDirectory delegate = FSDirectory.open(shardPath.resolveIndex());

            CompositeStoreDirectory compositeDirectory = new CompositeStoreDirectory(delegate, shardPath, checksumRegistry);

            if (logger.isDebugEnabled()) {
                logger.debug(
                    "Successfully created CompositeStoreDirectory for shard: {} with checksum handlers: {}",
                    shardPath.getShardId(),
                    checksumRegistry.getRegisteredFormats()
                );
            }

            return compositeDirectory;

        } catch (Exception e) {
            logger.error(
                () -> new org.apache.logging.log4j.message.ParameterizedMessage(
                    "Failed to create CompositeStoreDirectory for shard: {}",
                    shardPath.getShardId()
                ),
                e
            );
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Failed to create CompositeStoreDirectory for shard %s: %s",
                    shardPath.getShardId(),
                    e.getMessage()
                ),
                e
            );
        }
    }
}
