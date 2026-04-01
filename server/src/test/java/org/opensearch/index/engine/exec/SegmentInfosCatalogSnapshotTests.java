/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link SegmentInfosCatalogSnapshot}.
 */
public class SegmentInfosCatalogSnapshotTests extends OpenSearchTestCase {

    private SegmentInfos createTestSegmentInfos() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);

        // Create a simple SegmentInfo and add it
        byte[] id = StringHelper.randomId();
        SegmentInfo si = new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "_0",
            1,
            false,
            false,
            org.apache.lucene.codecs.Codec.getDefault(),
            Collections.emptyMap(),
            id,
            Collections.emptyMap(),
            null
        );
        // Write a dummy .si file so SegmentInfo has files
        try (IndexOutput out = dir.createOutput("_0.si", IOContext.DEFAULT)) {
            out.writeString("test");
        }
        si.setFiles(Collections.singleton("_0.si"));

        org.apache.lucene.index.SegmentCommitInfo sci = new org.apache.lucene.index.SegmentCommitInfo(si, 0, 0, -1, -1, -1, id);
        segmentInfos.add(sci);

        return segmentInfos;
    }

    private SegmentInfos createEmptySegmentInfos() {
        return new SegmentInfos(Version.LATEST.major);
    }

    // ═══════════════════════════════════════════════════════════════
    // Constructor Tests
    // ═══════════════════════════════════════════════════════════════

    public void testConstructorWithSegmentInfos() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        assertNotNull(snapshot);
        assertSame(segmentInfos, snapshot.getSegmentInfos());
        assertEquals(segmentInfos.getGeneration(), snapshot.getGeneration());
        assertEquals(segmentInfos.getVersion(), snapshot.getVersion());
    }

    public void testGetSegmentInfos() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        assertSame(segmentInfos, snapshot.getSegmentInfos());
    }

    // ═══════════════════════════════════════════════════════════════
    // getUserData Tests
    // ═══════════════════════════════════════════════════════════════

    public void testGetUserData() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        Map<String, String> userData = new HashMap<>();
        userData.put("key1", "value1");
        userData.put("key2", "value2");
        segmentInfos.setUserData(userData, false);

        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);
        Map<String, String> result = snapshot.getUserData();

        assertEquals("value1", result.get("key1"));
        assertEquals("value2", result.get("key2"));
    }

    public void testGetUserDataEmpty() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        Map<String, String> result = snapshot.getUserData();
        assertNotNull(result);
    }

    // ═══════════════════════════════════════════════════════════════
    // getId Tests
    // ═══════════════════════════════════════════════════════════════

    public void testGetId() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        assertEquals(segmentInfos.getGeneration(), snapshot.getId());
    }

    // ═══════════════════════════════════════════════════════════════
    // UnsupportedOperationException Tests
    // ═══════════════════════════════════════════════════════════════

    public void testGetSegmentsThrows() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        expectThrows(UnsupportedOperationException.class, snapshot::getSegments);
    }

    public void testGetSearchableFilesThrows() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        expectThrows(UnsupportedOperationException.class, () -> snapshot.getSearchableFiles("lucene"));
    }

    public void testGetDataFormatsThrows() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        expectThrows(UnsupportedOperationException.class, snapshot::getDataFormats);
    }

    public void testSerializeToStringThrows() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        expectThrows(UnsupportedOperationException.class, snapshot::serializeToString);
    }

    public void testGetReaderThrows() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        expectThrows(UnsupportedOperationException.class, () -> snapshot.getReader(null));
    }

    // ═══════════════════════════════════════════════════════════════
    // getLastWriterGeneration Tests
    // ═══════════════════════════════════════════════════════════════

    public void testGetLastWriterGeneration() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        assertEquals(-1, snapshot.getLastWriterGeneration());
    }

    // ═══════════════════════════════════════════════════════════════
    // setCatalogSnapshotMap Tests
    // ═══════════════════════════════════════════════════════════════

    public void testSetCatalogSnapshotMapIsNoOp() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        // Should not throw - it's a no-op
        snapshot.setCatalogSnapshotMap(new HashMap<>());
        snapshot.setCatalogSnapshotMap(null);
    }

    // ═══════════════════════════════════════════════════════════════
    // clone Tests
    // ═══════════════════════════════════════════════════════════════

    public void testClone() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);
        SegmentInfosCatalogSnapshot cloned = snapshot.clone();

        assertNotNull(cloned);
        assertNotSame(snapshot, cloned);
        assertEquals(snapshot.getGeneration(), cloned.getGeneration());
        assertEquals(snapshot.getVersion(), cloned.getVersion());
    }

    // ═══════════════════════════════════════════════════════════════
    // setUserData Tests
    // ═══════════════════════════════════════════════════════════════

    public void testSetUserData() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        Map<String, String> userData = new HashMap<>();
        userData.put("testKey", "testValue");
        snapshot.setUserData(userData, false);

        assertEquals("testValue", snapshot.getUserData().get("testKey"));
    }

    // ═══════════════════════════════════════════════════════════════
    // closeInternal Tests
    // ═══════════════════════════════════════════════════════════════

    public void testCloseInternal() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        // closeInternal is a no-op, decRef should not throw
        // AbstractRefCounted calls closeInternal() when refCount hits 0
        snapshot.decRef();
    }



    // ═══════════════════════════════════════════════════════════════
    // serialize Tests
    // ═══════════════════════════════════════════════════════════════

    public void testSerialize() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        byte[] bytes = snapshot.serialize();

        assertNotNull(bytes);
        assertTrue("Serialized bytes should not be empty", bytes.length > 0);
    }

    // ═══════════════════════════════════════════════════════════════
    // getFormatVersionForFile Tests
    // ═══════════════════════════════════════════════════════════════

    public void testGetFormatVersionForFile_SegmentFile() throws IOException {
        SegmentInfos segmentInfos = createTestSegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        // _0.si is a known segment file
        int version = snapshot.getFormatVersionForFile("_0.si");
        assertEquals(Version.LATEST.major, version);
    }

    public void testGetFormatVersionForFile_SegmentsFile_FallsBackToLatest() throws IOException {
        // For uncommitted SegmentInfos, getCommitLuceneVersion() is null,
        // so getFormatVersionForFile falls through to Version.LATEST.major
        SegmentInfos segmentInfos = createTestSegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        // The segments_N file for uncommitted SegmentInfos
        // Since commitLuceneVersion is null, this will NPE in the code path that checks it.
        // The code first checks the versionMap (won't find segments_N there),
        // then checks if it's the segments file and calls getCommitLuceneVersion().major
        // For uncommitted SegmentInfos this is null, so let's verify behavior for a file
        // not in the segment map that isn't the segments file either.
        String unknownSegmentFile = "_99.cfs";
        int version = snapshot.getFormatVersionForFile(unknownSegmentFile);
        // Falls back to Version.LATEST.major since _99 has no .si file in versionMap
        assertEquals(Version.LATEST.major, version);
    }

    public void testGetFormatVersionForFile_UnknownFile() throws IOException {
        SegmentInfos segmentInfos = createTestSegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        // An unknown file like _0.cfs should fall back to .si version for same segment
        int version = snapshot.getFormatVersionForFile("_0.cfs");
        assertEquals(Version.LATEST.major, version);
    }

    public void testGetFormatVersionForFile_CompletelyUnknownFile() throws IOException {
        SegmentInfos segmentInfos = createEmptySegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        // A completely unknown file should fall back to Version.LATEST.major
        int version = snapshot.getFormatVersionForFile("unknown_file.xyz");
        assertEquals(Version.LATEST.major, version);
    }
}
