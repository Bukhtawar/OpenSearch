/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;

/**
 * Unit tests for {@link DefaultCompositeStoreDirectoryFactory}.
 */
public class DefaultCompositeStoreDirectoryFactoryTests extends OpenSearchTestCase {

    private ShardPath createShardPath(Path tempDir) throws IOException {
        String indexUUID = "test-index-uuid";
        int shardId = 0;
        Path shardDataPath = tempDir.resolve(indexUUID).resolve(Integer.toString(shardId));
        Path indexPath = shardDataPath.resolve(ShardPath.INDEX_FOLDER_NAME);
        Files.createDirectories(indexPath);

        ShardId sid = new ShardId(new Index("test-index", indexUUID), shardId);
        return new ShardPath(false, shardDataPath, shardDataPath, sid);
    }

    private IndexSettings createIndexSettings() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(SETTING_INDEX_UUID, "test-index-uuid")
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        return new IndexSettings(metadata, Settings.EMPTY);
    }

    // ═══════════════════════════════════════════════════════════════
    // newCompositeStoreDirectory Tests
    // ═══════════════════════════════════════════════════════════════

    public void testNewCompositeStoreDirectory_CreatesSuccessfully() throws IOException {
        DefaultCompositeStoreDirectoryFactory factory = new DefaultCompositeStoreDirectoryFactory();
        Path tempDir = createTempDir();
        ShardPath shardPath = createShardPath(tempDir);
        IndexSettings indexSettings = createIndexSettings();

        CompositeStoreDirectory directory = factory.newCompositeStoreDirectory(indexSettings, shardPath.getShardId(), shardPath);

        assertNotNull("Factory should create a non-null CompositeStoreDirectory", directory);
    }

    public void testNewCompositeStoreDirectory_HasCorrectShardPath() throws IOException {
        DefaultCompositeStoreDirectoryFactory factory = new DefaultCompositeStoreDirectoryFactory();
        Path tempDir = createTempDir();
        ShardPath shardPath = createShardPath(tempDir);
        IndexSettings indexSettings = createIndexSettings();

        CompositeStoreDirectory directory = factory.newCompositeStoreDirectory(indexSettings, shardPath.getShardId(), shardPath);

        assertEquals(shardPath, directory.getShardPath());
    }

    public void testNewCompositeStoreDirectory_HasChecksumRegistry() throws IOException {
        DefaultCompositeStoreDirectoryFactory factory = new DefaultCompositeStoreDirectoryFactory();
        Path tempDir = createTempDir();
        ShardPath shardPath = createShardPath(tempDir);
        IndexSettings indexSettings = createIndexSettings();

        CompositeStoreDirectory directory = factory.newCompositeStoreDirectory(indexSettings, shardPath.getShardId(), shardPath);

        assertNotNull("Directory should have a checksum registry", directory.getChecksumRegistry());
        assertTrue("Checksum registry should have 'lucene' handler", directory.getChecksumRegistry().hasHandler("lucene"));
    }

    public void testNewCompositeStoreDirectory_CanListFiles() throws IOException {
        DefaultCompositeStoreDirectoryFactory factory = new DefaultCompositeStoreDirectoryFactory();
        Path tempDir = createTempDir();
        ShardPath shardPath = createShardPath(tempDir);
        IndexSettings indexSettings = createIndexSettings();

        CompositeStoreDirectory directory = factory.newCompositeStoreDirectory(indexSettings, shardPath.getShardId(), shardPath);

        // Should not throw
        String[] files = directory.listAll();
        assertNotNull(files);
    }

    public void testNewCompositeStoreDirectory_MultipleCalls_CreatesSeparateInstances() throws IOException {
        DefaultCompositeStoreDirectoryFactory factory = new DefaultCompositeStoreDirectoryFactory();
        Path tempDir1 = createTempDir();
        Path tempDir2 = createTempDir();
        ShardPath shardPath1 = createShardPath(tempDir1);
        ShardPath shardPath2 = createShardPath(tempDir2);
        IndexSettings indexSettings = createIndexSettings();

        CompositeStoreDirectory dir1 = factory.newCompositeStoreDirectory(indexSettings, shardPath1.getShardId(), shardPath1);
        CompositeStoreDirectory dir2 = factory.newCompositeStoreDirectory(indexSettings, shardPath2.getShardId(), shardPath2);

        assertNotNull(dir1);
        assertNotNull(dir2);
        assertNotSame("Each call should create a new instance", dir1, dir2);
    }

    public void testNewCompositeStoreDirectory_InvalidPath_ThrowsIOException() throws IOException {
        DefaultCompositeStoreDirectoryFactory factory = new DefaultCompositeStoreDirectoryFactory();
        IndexSettings indexSettings = createIndexSettings();

        // Create a valid shard path structure (must end with shardId, parent with indexUUID)
        // but place a regular file where the "index" directory should be, so FSDirectory.open() fails
        String indexUUID = "test-index-uuid";
        int shardId = 0;
        Path tempDir = createTempDir();
        Path shardDataPath = tempDir.resolve(indexUUID).resolve(Integer.toString(shardId));
        Files.createDirectories(shardDataPath);
        // Create a FILE named "index" instead of a directory — FSDirectory.open() will fail
        Path indexFile = shardDataPath.resolve(ShardPath.INDEX_FOLDER_NAME);
        Files.createFile(indexFile);

        ShardId sid = new ShardId(new Index("test-index", indexUUID), shardId);
        ShardPath invalidShardPath = new ShardPath(false, shardDataPath, shardDataPath, sid);

        // This should trigger the catch block which wraps the exception as IOException
        IOException exception = expectThrows(
            IOException.class,
            () -> factory.newCompositeStoreDirectory(indexSettings, invalidShardPath.getShardId(), invalidShardPath)
        );
        assertTrue(
            "Exception message should mention shard, but was: " + exception.getMessage(),
            exception.getMessage().contains("Failed to create CompositeStoreDirectory for shard")
        );
    }
}
