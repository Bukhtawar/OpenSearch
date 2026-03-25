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
import org.opensearch.plugins.PluginsService;

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
    public CompositeStoreDirectory newCompositeStoreDirectory(
        IndexSettings indexSettings,
        ShardId shardId, ShardPath shardPath
    ) throws IOException {

        if (logger.isDebugEnabled()) {
            logger.debug("Creating CompositeStoreDirectory for shard: {} at path: {}",
                shardPath.getShardId(), shardPath.getDataPath());
        }

        try {
            // Create FSDirectory on the shard's index/ directory as the delegate
            FSDirectory delegate = FSDirectory.open(shardPath.resolveIndex());

            CompositeStoreDirectory compositeDirectory = new CompositeStoreDirectory(
                delegate,
                shardPath
            );

            if (logger.isDebugEnabled()) {
                logger.debug("Successfully created CompositeStoreDirectory for shard: {}",
                    shardPath.getShardId()
                );
            }

            return compositeDirectory;

        } catch (Exception e) {
            logger.error("Failed to create CompositeStoreDirectory for shard: {}",
                shardPath.getShardId(), e);
            throw new IOException(
                String.format(Locale.ROOT, "Failed to create CompositeStoreDirectory for shard %s: %s",
                    shardPath.getShardId(), e.getMessage()),
                e
            );
        }
    }
}
