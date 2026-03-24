/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;

public class CompositeStoreDirectoryTests extends OpenSearchTestCase {

    private Path tempDir;
    private Path shardDataPath;
    private Path indexPath;
    private FSDirectory fsDirectory;
    private ShardPath shardPath;
    private CompositeStoreDirectory compositeStoreDirectory;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Create directory structure: tempDir/<indexUUID>/<shardId>/index/
        tempDir = createTempDir();
        String indexUUID = "test-index-uuid";
        int shardId = 0;
        shardDataPath = tempDir.resolve(indexUUID).resolve(Integer.toString(shardId));
        indexPath = shardDataPath.resolve(ShardPath.INDEX_FOLDER_NAME);
        Files.createDirectories(indexPath);

        fsDirectory = FSDirectory.open(indexPath);
        ShardId sid = new ShardId(new Index("test-index", indexUUID), shardId);
        shardPath = new ShardPath(false, shardDataPath, shardDataPath, sid);

        compositeStoreDirectory = new CompositeStoreDirectory(fsDirectory, sid, shardPath, Set.of("parquet", "arrow"));
    }

    @After
    public void tearDown() throws Exception {
        compositeStoreDirectory.close();
        fsDirectory.close();
        super.tearDown();
    }

    // ═══════════════════════════════════════════════════════════════
    // toFileMetadata / toFileIdentifier
    // ═══════════════════════════════════════════════════════════════

    public void testToFileMetadata_luceneFile() {
        FileMetadata fm = compositeStoreDirectory.toFileMetadata("_0.si");
        assertEquals("lucene", fm.dataFormat());
        assertEquals("_0.si", fm.file());
    }

    public void testToFileMetadata_prefixedFile() {
        FileMetadata fm = compositeStoreDirectory.toFileMetadata("parquet/data.parquet");
        assertEquals("parquet", fm.dataFormat());
        assertEquals("data.parquet", fm.file());
    }

    public void testToFileMetadata_arrowFile() {
        FileMetadata fm = compositeStoreDirectory.toFileMetadata("arrow/data.arrow");
        assertEquals("arrow", fm.dataFormat());
        assertEquals("data.arrow", fm.file());
    }

    public void testToFileIdentifier_lucene() {
        FileMetadata fm = new FileMetadata("lucene", "_0.si");
        String identifier = compositeStoreDirectory.toFileIdentifier(fm);
        assertEquals("_0.si", identifier);
    }

    public void testToFileIdentifier_metadata() {
        // "metadata" is treated as a default/index format, so no prefix is added
        FileMetadata fm = new FileMetadata("metadata", "meta_file.txt");
        String identifier = compositeStoreDirectory.toFileIdentifier(fm);
        assertEquals("meta_file.txt", identifier);
    }

    public void testToFileIdentifier_nonLucene() {
        FileMetadata fm = new FileMetadata("parquet", "data.parquet");
        String identifier = compositeStoreDirectory.toFileIdentifier(fm);
        assertEquals("parquet/data.parquet", identifier);
    }

    public void testToFileIdentifier_arrow() {
        FileMetadata fm = new FileMetadata("arrow", "data.arrow");
        String identifier = compositeStoreDirectory.toFileIdentifier(fm);
        assertEquals("arrow/data.arrow", identifier);
    }

    public void testRoundtrip_toFileMetadata_toFileIdentifier_lucene() {
        String original = "_0.cfe";
        FileMetadata fm = compositeStoreDirectory.toFileMetadata(original);
        String result = compositeStoreDirectory.toFileIdentifier(fm);
        assertEquals(original, result);
    }

    public void testRoundtrip_toFileMetadata_toFileIdentifier_nonLucene() {
        String original = "parquet/data.parquet";
        FileMetadata fm = compositeStoreDirectory.toFileMetadata(original);
        String result = compositeStoreDirectory.toFileIdentifier(fm);
        assertEquals(original, result);
    }

    // ═══════════════════════════════════════════════════════════════
    // getDataFormat
    // ═══════════════════════════════════════════════════════════════

    public void testGetDataFormat_lucene() {
        assertEquals("lucene", compositeStoreDirectory.getDataFormat("_0.cfe"));
    }

    public void testGetDataFormat_nonLucene() {
        assertEquals("arrow", compositeStoreDirectory.getDataFormat("arrow/data.arrow"));
    }

    public void testGetDataFormat_parquet() {
        assertEquals("parquet", compositeStoreDirectory.getDataFormat("parquet/data.parquet"));
    }

    // ═══════════════════════════════════════════════════════════════
    // createOutput / openInput - Lucene files
    // ═══════════════════════════════════════════════════════════════

    public void testCreateOutputAndOpenInput_lucene() throws IOException {
        String fileName = "_0_test.si";
        byte[] testData = "hello world lucene".getBytes();

        try (IndexOutput out = compositeStoreDirectory.createOutput(fileName, IOContext.DEFAULT)) {
            out.writeBytes(testData, testData.length);
        }

        try (IndexInput in = compositeStoreDirectory.openInput(fileName, IOContext.DEFAULT)) {
            byte[] readData = new byte[testData.length];
            in.readBytes(readData, 0, readData.length);
            assertArrayEquals(testData, readData);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // createOutput / openInput - Non-Lucene files
    // ═══════════════════════════════════════════════════════════════

    public void testCreateOutputAndOpenInput_nonLucene() throws IOException {
        String fileIdentifier = "parquet/data.parquet";
        byte[] testData = "hello world parquet".getBytes();

        try (IndexOutput out = compositeStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeBytes(testData, testData.length);
        }

        try (IndexInput in = compositeStoreDirectory.openInput(fileIdentifier, IOContext.DEFAULT)) {
            byte[] readData = new byte[testData.length];
            in.readBytes(readData, 0, readData.length);
            assertArrayEquals(testData, readData);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // fileLength
    // ═══════════════════════════════════════════════════════════════

    public void testFileLength_lucene() throws IOException {
        String fileName = "_test_len.si";
        byte[] data = "some content for length test".getBytes();

        try (IndexOutput out = compositeStoreDirectory.createOutput(fileName, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        assertEquals(data.length, compositeStoreDirectory.fileLength(fileName));
    }

    public void testFileLength_fileMetadata() throws IOException {
        String fileIdentifier = "parquet/len_test.parquet";
        byte[] data = "parquet length test".getBytes();

        try (IndexOutput out = compositeStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        FileMetadata fm = new FileMetadata("parquet", "len_test.parquet");
        assertEquals(data.length, compositeStoreDirectory.fileLength(fm));
    }

    // ═══════════════════════════════════════════════════════════════
    // deleteFile
    // ═══════════════════════════════════════════════════════════════

    public void testDeleteFile_string() throws IOException {
        String fileName = "_del_test.si";
        try (IndexOutput out = compositeStoreDirectory.createOutput(fileName, IOContext.DEFAULT)) {
            out.writeString("to be deleted");
        }
        assertTrue(Arrays.asList(compositeStoreDirectory.listAll()).contains(fileName));

        compositeStoreDirectory.deleteFile(fileName);
        assertFalse(Arrays.asList(compositeStoreDirectory.listAll()).contains(fileName));
    }

    public void testDeleteFile_fileMetadata() throws IOException {
        String fileIdentifier = "parquet/del_test.parquet";
        try (IndexOutput out = compositeStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeString("to be deleted");
        }
        assertTrue(Arrays.asList(compositeStoreDirectory.listAll()).contains(fileIdentifier));

        FileMetadata fm = new FileMetadata("parquet", "del_test.parquet");
        compositeStoreDirectory.deleteFile(fm);
        assertFalse(Arrays.asList(compositeStoreDirectory.listAll()).contains(fileIdentifier));
    }

    // ═══════════════════════════════════════════════════════════════
    // listAll / listFileMetadata
    // ═══════════════════════════════════════════════════════════════

    public void testListAll_empty() throws IOException {
        // A fresh directory should list no segment files
        String[] files = compositeStoreDirectory.listAll();
        assertNotNull(files);
    }

    public void testListAll_withFiles() throws IOException {
        try (IndexOutput out = compositeStoreDirectory.createOutput("_0.si", IOContext.DEFAULT)) {
            out.writeString("data");
        }
        try (IndexOutput out = compositeStoreDirectory.createOutput("parquet/data.parquet", IOContext.DEFAULT)) {
            out.writeString("parquet data");
        }

        String[] files = compositeStoreDirectory.listAll();
        List<String> fileList = Arrays.asList(files);
        assertTrue(fileList.contains("_0.si"));
        assertTrue(fileList.contains("parquet/data.parquet"));
    }

    public void testListFileMetadata() throws IOException {
        try (IndexOutput out = compositeStoreDirectory.createOutput("_0.si", IOContext.DEFAULT)) {
            out.writeString("data");
        }
        try (IndexOutput out = compositeStoreDirectory.createOutput("parquet/data.parquet", IOContext.DEFAULT)) {
            out.writeString("parquet data");
        }

        FileMetadata[] metadataArray = compositeStoreDirectory.listFileMetadata();
        assertNotNull(metadataArray);
        assertTrue(metadataArray.length >= 2);

        boolean foundLucene = false;
        boolean foundParquet = false;
        for (FileMetadata fm : metadataArray) {
            if ("lucene".equals(fm.dataFormat()) && "_0.si".equals(fm.file())) {
                foundLucene = true;
            }
            if ("parquet".equals(fm.dataFormat()) && "data.parquet".equals(fm.file())) {
                foundParquet = true;
            }
        }
        assertTrue("Should find lucene file in metadata", foundLucene);
        assertTrue("Should find parquet file in metadata", foundParquet);
    }

    // ═══════════════════════════════════════════════════════════════
    // Checksum - Lucene file (CodecUtil path)
    // ═══════════════════════════════════════════════════════════════

    public void testCalculateChecksum_luceneFile() throws IOException {
        String fileName = "_cksum.si";
        // Write a file with Lucene CodecUtil header/footer so CodecUtil.retrieveChecksum works
        try (IndexOutput out = compositeStoreDirectory.createOutput(fileName, IOContext.DEFAULT)) {
            CodecUtil.writeHeader(out, "TestCodec", 1);
            out.writeString("some segment data for checksum");
            CodecUtil.writeFooter(out);
        }

        long checksum = compositeStoreDirectory.calculateChecksum(fileName);
        // Verify it's a valid non-zero checksum (CodecUtil stores checksum in footer)
        assertTrue("Checksum should be a valid value", checksum != 0);

        // Verify we get the same checksum via FileMetadata overload
        FileMetadata fm = compositeStoreDirectory.toFileMetadata(fileName);
        assertEquals(checksum, compositeStoreDirectory.calculateChecksum(fm));
    }

    // ═══════════════════════════════════════════════════════════════
    // Checksum - Non-Lucene file (CRC32 path)
    // ═══════════════════════════════════════════════════════════════

    public void testCalculateChecksum_nonLuceneFile() throws IOException {
        String fileIdentifier = "parquet/cksum.parquet";
        byte[] data = "parquet content for checksum".getBytes();

        try (IndexOutput out = compositeStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        long checksum = compositeStoreDirectory.calculateChecksum(fileIdentifier);

        // Compute expected CRC32 manually
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        assertEquals("CRC32 checksum should match", crc32.getValue(), checksum);
    }

    // ═══════════════════════════════════════════════════════════════
    // calculateUploadChecksum
    // ═══════════════════════════════════════════════════════════════

    public void testCalculateUploadChecksum() throws IOException {
        String fileIdentifier = "parquet/upload_cksum.parquet";
        byte[] data = "upload checksum test".getBytes();

        try (IndexOutput out = compositeStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        FileMetadata fm = new FileMetadata("parquet", "upload_cksum.parquet");
        String uploadChecksum = compositeStoreDirectory.calculateUploadChecksum(fm);
        assertNotNull(uploadChecksum);
        // Should be the string representation of the long checksum
        long parsedChecksum = Long.parseLong(uploadChecksum);
        assertEquals(compositeStoreDirectory.calculateChecksum(fm), parsedChecksum);
    }

    // ═══════════════════════════════════════════════════════════════
    // rename
    // ═══════════════════════════════════════════════════════════════

    public void testRename_sameFormat() throws IOException {
        String fileName = "_rename_src.si";
        try (IndexOutput out = compositeStoreDirectory.createOutput(fileName, IOContext.DEFAULT)) {
            out.writeString("rename test data");
        }

        compositeStoreDirectory.rename(fileName, "_rename_dest.si");
        assertFalse(Arrays.asList(compositeStoreDirectory.listAll()).contains(fileName));
        assertTrue(Arrays.asList(compositeStoreDirectory.listAll()).contains("_rename_dest.si"));
    }

    public void testRename_fileMetadata_sameFormat() throws IOException {
        String fileIdentifier = "parquet/rename_src.parquet";
        try (IndexOutput out = compositeStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeString("rename test data parquet");
        }

        FileMetadata src = new FileMetadata("parquet", "rename_src.parquet");
        FileMetadata dest = new FileMetadata("parquet", "rename_dest.parquet");
        compositeStoreDirectory.rename(src, dest);

        assertFalse(Arrays.asList(compositeStoreDirectory.listAll()).contains("parquet/rename_src.parquet"));
        assertTrue(Arrays.asList(compositeStoreDirectory.listAll()).contains("parquet/rename_dest.parquet"));
    }

    public void testRename_crossFormat_throws() {
        FileMetadata src = new FileMetadata("parquet", "data.parquet");
        FileMetadata dest = new FileMetadata("arrow", "data.arrow");

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> compositeStoreDirectory.rename(src, dest)
        );
        assertTrue(ex.getMessage().contains("Cannot rename across formats"));
    }

    // ═══════════════════════════════════════════════════════════════
    // sync
    // ═══════════════════════════════════════════════════════════════

    public void testSync() throws IOException {
        String fileName = "_sync_test.si";
        try (IndexOutput out = compositeStoreDirectory.createOutput(fileName, IOContext.DEFAULT)) {
            out.writeString("sync test data");
        }

        // sync should not throw
        compositeStoreDirectory.sync(Set.of(fileName));
    }

    public void testGetShardPath() {
        assertNotNull(compositeStoreDirectory.getShardPath());
        assertEquals(shardPath, compositeStoreDirectory.getShardPath());
    }

    // ═══════════════════════════════════════════════════════════════
    // FileMetadata convenience: getChecksumOfLocalFile
    // ═══════════════════════════════════════════════════════════════

    public void testGetChecksumOfLocalFile() throws IOException {
        String fileIdentifier = "parquet/local_cksum.parquet";
        byte[] data = "local checksum test".getBytes();

        try (IndexOutput out = compositeStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        FileMetadata fm = new FileMetadata("parquet", "local_cksum.parquet");
        long checksum = compositeStoreDirectory.getChecksumOfLocalFile(fm);
        assertEquals(compositeStoreDirectory.calculateChecksum(fm), checksum);
    }

    // ═══════════════════════════════════════════════════════════════
    // resolveFileName with FileMetadata.DELIMITER in name
    // ═══════════════════════════════════════════════════════════════

    public void testOpenInput_withSerializedFileMetadata() throws IOException {
        // Write a file using plain identifier
        String fileIdentifier = "parquet/delimited_test.parquet";
        byte[] data = "delimited test data".getBytes();
        try (IndexOutput out = compositeStoreDirectory.createOutput(fileIdentifier, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        // Now access it using a serialized FileMetadata string (with ":::" delimiter)
        FileMetadata fm = new FileMetadata("parquet", "delimited_test.parquet");
        String serialized = fm.serialize(); // "delimited_test.parquet:::parquet"

        // The resolveFileName method should handle the delimiter and resolve correctly
        long length = compositeStoreDirectory.fileLength(serialized);
        assertEquals(data.length, length);
    }
}
