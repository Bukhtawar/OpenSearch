/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.test.OpenSearchTestCase;

public class FileMetadataTests extends OpenSearchTestCase {

    // ═══════════════════════════════════════════════════════════════
    // Two-arg constructor tests
    // ═══════════════════════════════════════════════════════════════

    public void testConstructorTwoArgs() {
        FileMetadata fm = new FileMetadata("parquet", "_0_1.parquet");
        assertEquals("parquet", fm.dataFormat());
        assertEquals("_0_1.parquet", fm.file());
    }

    public void testConstructorTwoArgs_lucene() {
        FileMetadata fm = new FileMetadata("lucene", "_0.si");
        assertEquals("lucene", fm.dataFormat());
        assertEquals("_0.si", fm.file());
    }

    // ═══════════════════════════════════════════════════════════════
    // Single-arg constructor tests
    // ═══════════════════════════════════════════════════════════════

    public void testConstructorSingleArg_luceneFileWithoutDelimiter() {
        // A plain filename without delimiter defaults to "lucene"
        FileMetadata fm = new FileMetadata("_0.si");
        assertEquals("lucene", fm.dataFormat());
        assertEquals("_0.si", fm.file());
    }

    public void testConstructorSingleArg_withDelimiter() {
        FileMetadata fm = new FileMetadata("_0_1.parquet:::parquet");
        assertEquals("parquet", fm.dataFormat());
        assertEquals("_0_1.parquet", fm.file());
    }

    public void testConstructorSingleArg_withDelimiterLucene() {
        FileMetadata fm = new FileMetadata("_0.si:::lucene");
        assertEquals("lucene", fm.dataFormat());
        assertEquals("_0.si", fm.file());
    }

    public void testConstructorSingleArg_metadataKey() {
        // Files starting with "metadata" and not containing delimiter are treated as metadata format
        FileMetadata fm = new FileMetadata("metadata__1__2__3");
        assertEquals("metadata", fm.dataFormat());
        assertEquals("metadata__1__2__3", fm.file());
    }

    public void testConstructorSingleArg_metadataKeyWithDelimiter() {
        // If it contains delimiter, parse normally even if it starts with "metadata"
        FileMetadata fm = new FileMetadata("metadata__1__2__3:::metadata");
        assertEquals("metadata", fm.dataFormat());
        assertEquals("metadata__1__2__3", fm.file());
    }

    // ═══════════════════════════════════════════════════════════════
    // serialize / toString
    // ═══════════════════════════════════════════════════════════════

    public void testSerialize() {
        FileMetadata fm = new FileMetadata("parquet", "_0_1.parquet");
        assertEquals("_0_1.parquet:::parquet", fm.serialize());
    }

    public void testSerialize_lucene() {
        FileMetadata fm = new FileMetadata("lucene", "_0.si");
        assertEquals("_0.si:::lucene", fm.serialize());
    }

    public void testToStringEqualsSerialized() {
        FileMetadata fm = new FileMetadata("arrow", "data.arrow");
        assertEquals(fm.serialize(), fm.toString());
    }

    // ═══════════════════════════════════════════════════════════════
    // Roundtrip: two-arg -> serialize -> single-arg
    // ═══════════════════════════════════════════════════════════════

    public void testRoundtrip() {
        FileMetadata original = new FileMetadata("parquet", "_0_1.parquet");
        String serialized = original.serialize();
        FileMetadata deserialized = new FileMetadata(serialized);
        assertEquals(original, deserialized);
        assertEquals(original.file(), deserialized.file());
        assertEquals(original.dataFormat(), deserialized.dataFormat());
    }

    public void testRoundtrip_lucene() {
        FileMetadata original = new FileMetadata("lucene", "_0.cfs");
        String serialized = original.serialize();
        FileMetadata deserialized = new FileMetadata(serialized);
        assertEquals(original, deserialized);
    }

    // ═══════════════════════════════════════════════════════════════
    // equals / hashCode
    // ═══════════════════════════════════════════════════════════════

    public void testEquals_sameValues() {
        FileMetadata fm1 = new FileMetadata("parquet", "_0.parquet");
        FileMetadata fm2 = new FileMetadata("parquet", "_0.parquet");
        assertEquals(fm1, fm2);
        assertEquals(fm1.hashCode(), fm2.hashCode());
    }

    public void testNotEqual_differentFile() {
        FileMetadata fm1 = new FileMetadata("parquet", "_0.parquet");
        FileMetadata fm2 = new FileMetadata("parquet", "_1.parquet");
        assertNotEquals(fm1, fm2);
    }

    public void testNotEqual_differentFormat() {
        FileMetadata fm1 = new FileMetadata("parquet", "_0.parquet");
        FileMetadata fm2 = new FileMetadata("arrow", "_0.parquet");
        assertNotEquals(fm1, fm2);
    }

    public void testEquals_null() {
        FileMetadata fm = new FileMetadata("lucene", "_0.si");
        assertNotEquals(null, fm);
    }

    public void testEquals_differentType() {
        FileMetadata fm = new FileMetadata("lucene", "_0.si");
        assertNotEquals("_0.si:::lucene", fm);
    }

    public void testEquals_self() {
        FileMetadata fm = new FileMetadata("lucene", "_0.si");
        assertEquals(fm, fm);
    }

    // ═══════════════════════════════════════════════════════════════
    // DELIMITER constant
    // ═══════════════════════════════════════════════════════════════

    public void testDelimiterConstant() {
        assertEquals(":::", FileMetadata.DELIMITER);
    }
}
