/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.checksum;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * Unit tests for {@link ChecksumHandler} implementations:
 * {@link LuceneChecksumHandler} and {@link GenericCRC32ChecksumHandler}.
 */
public class ChecksumHandlerTests extends OpenSearchTestCase {

    // ═══════════════════════════════════════════════════════════════
    // GenericCRC32ChecksumHandler Tests
    // ═══════════════════════════════════════════════════════════════

    public void testGenericCRC32ChecksumHandler_DefaultFormatName() {
        GenericCRC32ChecksumHandler handler = new GenericCRC32ChecksumHandler();
        assertEquals("generic", handler.getFormatName());
    }

    public void testGenericCRC32ChecksumHandler_CustomFormatName() {
        GenericCRC32ChecksumHandler handler = new GenericCRC32ChecksumHandler("parquet");
        assertEquals("parquet", handler.getFormatName());
    }

    public void testGenericCRC32ChecksumHandler_EmptyFile() throws IOException {
        GenericCRC32ChecksumHandler handler = new GenericCRC32ChecksumHandler();
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        try (IndexOutput out = dir.createOutput("empty.dat", IOContext.DEFAULT)) {
            // write nothing
        }

        try (IndexInput input = dir.openInput("empty.dat", IOContext.READONCE)) {
            long checksum = handler.calculateChecksum(input);
            CRC32 crc32 = new CRC32();
            assertEquals("CRC32 of empty file should match", crc32.getValue(), checksum);
        }
    }

    public void testGenericCRC32ChecksumHandler_LargeFile() throws IOException {
        GenericCRC32ChecksumHandler handler = new GenericCRC32ChecksumHandler();
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        // Write a file larger than the 8192 buffer size
        byte[] data = new byte[20000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        try (IndexOutput out = dir.createOutput("large.dat", IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        try (IndexInput input = dir.openInput("large.dat", IOContext.READONCE)) {
            long checksum = handler.calculateChecksum(input);

            CRC32 crc32 = new CRC32();
            crc32.update(data);
            assertEquals("CRC32 of large file should match", crc32.getValue(), checksum);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // LuceneChecksumHandler Tests
    // ═══════════════════════════════════════════════════════════════

    public void testLuceneChecksumHandler_FormatName() {
        LuceneChecksumHandler handler = new LuceneChecksumHandler();
        assertEquals("lucene", handler.getFormatName());
    }

    public void testLuceneChecksumHandler_CalculateChecksum() throws IOException {
        LuceneChecksumHandler handler = new LuceneChecksumHandler();
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        try (IndexOutput out = dir.createOutput("lucene_test.si", IOContext.DEFAULT)) {
            CodecUtil.writeHeader(out, "TestCodec", 1);
            out.writeString("some lucene data");
            CodecUtil.writeFooter(out);
        }

        try (IndexInput input = dir.openInput("lucene_test.si", IOContext.READONCE)) {
            long checksum = handler.calculateChecksum(input);
            assertTrue("Lucene checksum should be non-zero", checksum != 0);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // ChecksumHandler default method Tests
    // ═══════════════════════════════════════════════════════════════

    public void testChecksumHandlerDefaultUploadChecksum() throws IOException {
        GenericCRC32ChecksumHandler handler = new GenericCRC32ChecksumHandler();
        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        byte[] data = "test".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dir.createOutput("default_upload.dat", IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        try (IndexInput input = dir.openInput("default_upload.dat", IOContext.READONCE)) {
            String uploadChecksum = handler.calculateUploadChecksum(input);
            assertNotNull(uploadChecksum);
            // Should be the string representation of the long checksum
            Long.parseLong(uploadChecksum); // should not throw
        }
    }
}
