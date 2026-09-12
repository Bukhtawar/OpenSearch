/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * End-to-end tests for the storage-neutral {@code codec} and {@code bloom_filter} mapping parameters on composite
 * indices with Parquet as the primary format.
 *
 * <p>Covers the full path in a live cluster: the Parquet plugin contributes the parameters through
 * {@code DataFormatRegistry} / {@code CompositeDataFormatPlugin} fan-out, the core keyword, text, number and date
 * mappers accept them, they round-trip through {@code GET _mapping}, the plugin rejects encodings it cannot honour
 * for the field type, documents index through the write path, and the deprecated per-field index settings keep
 * working alongside the mapping parameters. Also verifies the parameters are unavailable on indices that do not
 * use the Parquet format.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeFieldCodecIT extends AbstractCompositeEngineIT {

    private static final String INDEX_NAME = "test-field-codec";

    public void testCodecAndBloomFilterRoundTripOnParquetPrimary() throws IOException {
        createCodecIndex(compositeSettings("parquet", List.of("lucene")));
        ensureGreen(INDEX_NAME);

        Map<String, Object> properties = getMappingProperties(INDEX_NAME);

        Map<String, Object> timestamp = fieldMapping(properties, "@timestamp");
        assertEquals("date", timestamp.get("type"));
        assertEquals(List.of("delta", "zstd(3)"), timestamp.get("codec"));

        Map<String, Object> traceId = fieldMapping(properties, "trace_id");
        assertEquals("keyword", traceId.get("type"));
        assertEquals("zstd(1)", traceId.get("codec"));
        assertEquals(Boolean.TRUE, traceId.get("bloom_filter"));

        Map<String, Object> bytes = fieldMapping(properties, "bytes");
        assertEquals("long", bytes.get("type"));
        assertEquals(List.of("delta", "lz4"), bytes.get("codec"));

        Map<String, Object> body = fieldMapping(properties, "body");
        assertEquals("text", body.get("type"));
        assertEquals("zstd(3)", body.get("codec"));

        // Control field: nothing declared → neither parameter appears in the mapping.
        Map<String, Object> userId = fieldMapping(properties, "user_id");
        assertNull(userId.get("codec"));
        assertNull(userId.get("bloom_filter"));

        indexDocs(INDEX_NAME, 5);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);
    }

    public void testUnsupportedEncodingForFieldTypeRejected() {
        Settings.Builder settings = compositeSettings("parquet", List.of("lucene"));
        Exception e = expectThrows(
            Exception.class,
            () -> client().admin()
                .indices()
                .prepareCreate(INDEX_NAME)
                .setSettings(settings)
                .setMapping("trace_id", "type=keyword,codec=byte_split")
                .get()
        );
        assertTrue(
            "unexpected message: " + e.getMessage(),
            e.getMessage().contains("codec encoding [byte_split] is not supported for fields of type [keyword]")
        );
    }

    public void testUnknownCodecTokenRejected() {
        Settings.Builder settings = compositeSettings("parquet", List.of("lucene"));
        Exception e = expectThrows(
            Exception.class,
            () -> client().admin()
                .indices()
                .prepareCreate(INDEX_NAME)
                .setSettings(settings)
                .setMapping("bytes", "type=long,codec=brotli")
                .get()
        );
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("unknown codec token [brotli]"));
    }

    /**
     * When Parquet is only a secondary format, the parameters parse but the index-creation validator rejects them,
     * because per-field column configuration requires Parquet to be the primary format.
     */
    public void testCodecRejectedWhenParquetNotPrimary() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createCodecIndex(compositeSettings("lucene", List.of("parquet")))
        );
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("does not use parquet data format"));
    }

    /** On a plain index the parameters are never contributed, so the mapping fails to parse as unknown. */
    public void testCodecUnknownOnPlainIndex() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        Exception e = expectThrows(
            Exception.class,
            () -> client().admin()
                .indices()
                .prepareCreate(INDEX_NAME)
                .setSettings(settings)
                .setMapping("bytes", "type=long,codec=zstd")
                .get()
        );
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("unknown parameter [codec]"));
    }

    /**
     * The deprecated per-field index settings remain accepted and can coexist with mapping-level parameters on the
     * same index, including on the same field (the mapping value wins at write time).
     */
    public void testDeprecatedSettingsCoexistWithMappingParameters() throws IOException {
        Settings.Builder settings = compositeSettings("parquet", List.of("lucene")).putList(
            "index.parquet.compression.field",
            "user_id",
            "trace_id"
        )
            .putList("index.parquet.compression.value", "SNAPPY", "SNAPPY")
            .putList("index.parquet.bloom_filter_enabled.field", "user_id")
            .putList("index.parquet.bloom_filter_enabled.value", "true");
        createCodecIndex(settings);
        ensureGreen(INDEX_NAME);

        Map<String, Object> properties = getMappingProperties(INDEX_NAME);
        assertEquals("zstd(1)", fieldMapping(properties, "trace_id").get("codec"));
        assertNull(fieldMapping(properties, "user_id").get("codec"));

        indexDocs(INDEX_NAME, 5);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);
    }

    // --- Helpers ---

    private Settings.Builder compositeSettings(String primary, List<String> secondaries) {
        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", primary);
        if (secondaries.isEmpty()) {
            builder.putList("index.composite.secondary_data_formats");
        } else {
            builder.putList("index.composite.secondary_data_formats", secondaries.toArray(new String[0]));
        }
        return builder;
    }

    private void createCodecIndex(Settings.Builder settings) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .field("codec", List.of("delta", "zstd(3)"))
            .endObject()
            .startObject("trace_id")
            .field("type", "keyword")
            .field("index", false)
            .field("codec", "zstd(1)")
            .field("bloom_filter", true)
            .endObject()
            .startObject("bytes")
            .field("type", "long")
            .field("codec", List.of("delta", "lz4"))
            .endObject()
            .startObject("body")
            .field("type", "text")
            .field("index", false)
            .field("codec", "zstd(3)")
            .endObject()
            .startObject("user_id")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();

        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).setMapping(mapping).get();
    }

    private void indexDocs(String indexName, int count) {
        for (int i = 0; i < count; i++) {
            RestStatus status = client().prepareIndex()
                .setIndex(indexName)
                .setSource(
                    "@timestamp",
                    1_700_000_000_000L + i,
                    "trace_id",
                    "trace_" + i,
                    "bytes",
                    1024L * i,
                    "body",
                    "request body " + i,
                    "user_id",
                    "user_" + i
                )
                .get()
                .status();
            assertEquals(RestStatus.CREATED, status);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMappingProperties(String indexName) {
        GetMappingsResponse response = client().admin().indices().prepareGetMappings(indexName).get();
        MappingMetadata mappingMetadata = response.getMappings().get(indexName);
        assertNotNull("mapping metadata must exist for " + indexName, mappingMetadata);
        Map<String, Object> source = mappingMetadata.getSourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) source.get("properties");
        assertNotNull("mapping must contain properties", properties);
        return properties;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> fieldMapping(Map<String, Object> properties, String field) {
        Map<String, Object> mapping = (Map<String, Object>) properties.get(field);
        assertNotNull("field [" + field + "] must be present in mapping", mapping);
        return mapping;
    }
}
