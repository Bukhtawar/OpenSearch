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
import java.util.Set;
import java.util.zip.CRC32;

/**
 * Unit tests for {@link ChecksumHandlerRegistry}.
 */
public class ChecksumHandlerRegistryTests extends OpenSearchTestCase {

    // ═══════════════════════════════════════════════════════════════
    // Constructor / Initialization Tests
    // ═══════════════════════════════════════════════════════════════

    public void testDefaultConstructorRegistersLucene() {
        ChecksumHandlerRegistry registry = new ChecksumHandlerRegistry();

        assertTrue("Registry should have 'lucene' handler", registry.hasHandler("lucene"));
        Set<String> formats = registry.getRegisteredFormats();
        assertTrue("Registered formats should contain 'lucene'", formats.contains("lucene"));
    }

    public void testDefaultConstructorDoesNotRegisterOtherFormats() {
        ChecksumHandlerRegistry registry = new ChecksumHandlerRegistry();

        assertFalse("Registry should not have 'parquet' handler", registry.hasHandler("parquet"));
        assertFalse("Registry should not have 'arrow' handler", registry.hasHandler("arrow"));
    }

    // ═══════════════════════════════════════════════════════════════
    // registerHandler Tests
    // ═══════════════════════════════════════════════════════════════

    public void testRegisterHandler() {
        ChecksumHandlerRegistry registry = new ChecksumHandlerRegistry();
        ChecksumHandler handler = new GenericCRC32ChecksumHandler("parquet");

        registry.registerHandler(handler);

        assertTrue("Registry should have 'parquet' handler after registration", registry.hasHandler("parquet"));
        assertSame(handler, registry.getHandler("parquet"));
    }

    public void testRegisterHandlerNullThrows() {
        ChecksumHandlerRegistry registry = new ChecksumHandlerRegistry();

        expectThrows(IllegalArgumentException.class, () -> registry.registerHandler(null));
    }

    public void testRegisterHandlerOverwrite() {
        ChecksumHandlerRegistry registry = new ChecksumHandlerRegistry();
        ChecksumHandler handler1 = new GenericCRC32ChecksumHandler("custom");
        ChecksumHandler handler2 = new GenericCRC32ChecksumHandler("custom");

        registry.registerHandler(handler1);
        registry.registerHandler(handler2);

        assertSame("Second handler should overwrite the first", handler2, registry.getHandler("custom"));
    }

    // ═══════════════════════════════════════════════════════════════
    // getHandler Tests
    // ═══════════════════════════════════════════════════════════════

    public void testGetHandlerRegistered() {
        ChecksumHandlerRegistry registry = new ChecksumHandlerRegistry();

        ChecksumHandler handler = registry.getHandler("lucene");
        assertNotNull(handler);
        assertTrue("Lucene handler should be LuceneChecksumHandler", handler instanceof LuceneChecksumHandler);
    }

    public void testGetHandlerUnregistered_ReturnsDefault() {
        ChecksumHandlerRegistry registry = new ChecksumHandlerRegistry();

        ChecksumHandler handler = registry.getHandler("unknown_format");
        assertNotNull("Should return default handler for unknown format", handler);
        assertTrue("Default handler should be GenericCRC32ChecksumHandler", handler instanceof GenericCRC32ChecksumHandler);
    }

    // ═══════════════════════════════════════════════════════════════
    // hasHandler Tests
    // ═══════════════════════════════════════════════════════════════

    public void testHasHandlerTrue() {
        ChecksumHandlerRegistry registry = new ChecksumHandlerRegistry();
        assertTrue(registry.hasHandler("lucene"));
    }

    public void testHasHandlerFalse() {
        ChecksumHandlerRegistry registry = new ChecksumHandlerRegistry();
        assertFalse(registry.hasHandler("nonexistent"));
    }

    // ═══════════════════════════════════════════════════════════════
    // getRegisteredFormats Tests
    // ═══════════════════════════════════════════════════════════════

    public void testGetRegisteredFormats() {
        ChecksumHandlerRegistry registry = new ChecksumHandlerRegistry();
        registry.registerHandler(new GenericCRC32ChecksumHandler("parquet"));
        registry.registerHandler(new GenericCRC32ChecksumHandler("arrow"));

        Set<String> formats = registry.getRegisteredFormats();
        assertTrue(formats.contains("lucene"));
        assertTrue(formats.contains("parquet"));
        assertTrue(formats.contains("arrow"));
        assertEquals(3, formats.size());
    }

    public void testGetRegisteredFormatsUnmodifiable() {
        ChecksumHandlerRegistry registry = new ChecksumHandlerRegistry();
        Set<String> formats = registry.getRegisteredFormats();

        expectThrows(UnsupportedOperationException.class, () -> formats.add("should_fail"));
    }

    // ═══════════════════════════════════════════════════════════════
    // calculateChecksum Tests
    // ═══════════════════════════════════════════════════════════════

    public void testCalculateChecksum_LuceneFormat() throws IOException {
        ChecksumHandlerRegistry registry = new ChecksumHandlerRegistry();
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        // Write a file with Lucene codec footer
        try (IndexOutput out = dir.createOutput("test.si", IOContext.DEFAULT)) {
            CodecUtil.writeHeader(out, "TestCodec", 1);
            out.writeString("test data");
            CodecUtil.writeFooter(out);
        }

        try (IndexInput input = dir.openInput("test.si", IOContext.READONCE)) {
            long checksum = registry.calculateChecksum("lucene", input);
            assertTrue("Checksum should be non-zero", checksum != 0);
        }
    }

    public void testCalculateChecksum_UnknownFormat_FallsBackToCRC32() throws IOException {
        ChecksumHandlerRegistry registry = new ChecksumHandlerRegistry();
        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        byte[] data = "test content for crc32".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dir.createOutput("test.parquet", IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        try (IndexInput input = dir.openInput("test.parquet", IOContext.READONCE)) {
            long checksum = registry.calculateChecksum("parquet", input);

            // Manually compute CRC32
            CRC32 crc32 = new CRC32();
            crc32.update(data);
            assertEquals("CRC32 checksum should match", crc32.getValue(), checksum);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // calculateUploadChecksum Tests
    // ═══════════════════════════════════════════════════════════════

    public void testCalculateUploadChecksum() throws IOException {
        ChecksumHandlerRegistry registry = new ChecksumHandlerRegistry();
        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        byte[] data = "upload checksum data".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = dir.createOutput("test.dat", IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }

        try (IndexInput input = dir.openInput("test.dat", IOContext.READONCE)) {
            String checksum = registry.calculateUploadChecksum("generic", input);
            assertNotNull(checksum);

            // Should be parseable as long
            long parsedChecksum = Long.parseLong(checksum);

            CRC32 crc32 = new CRC32();
            crc32.update(data);
            assertEquals(crc32.getValue(), parsedChecksum);
        }
    }

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
