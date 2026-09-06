/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.exec.DocumentMetadataResolver;
import org.opensearch.index.engine.exec.DocumentMetadataResolver.DocumentMetadata;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.get.DocumentLookupResult;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.indices.IndicesModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Core orchestrator for document lookup. Coordinates row-location resolution,
 * backend-specific execution, and result assembly.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DocumentLookupService {

    /**
     * Engine/storage metadata fields excluded from reconstructed {@code _source}, sourced from the mapper
     * registry's built-in metadata mappers rather than a hand-maintained list. {@code _primary_term} (a
     * sub-column emitted by the {@code _seq_no} mapper) and {@code __row_id__} (an engine-internal column)
     * are not registered mappers, so they are excluded separately by constant in {@link #buildResultFromRow}.
     */
    private static final Set<String> METADATA_FIELDS = IndicesModule.getBuiltInMetadataFields();

    private final DocumentMetadataResolver documentResolver;
    private final DocumentRowReader executor;

    public DocumentLookupService(DocumentMetadataResolver documentResolver, DocumentRowReader executor) {
        this.documentResolver = documentResolver;
        this.executor = executor;
    }

    public DocumentLookupResult getById(String id, IndexReaderProvider.Reader reader, Index index) throws IOException {
        DocumentMetadata metadata = documentResolver.resolveMetadata(reader, id);
        if (metadata == null) {
            return DocumentLookupResult.notFound(id);
        }
        return buildResultFromRow(id, fetchRow(metadata, reader));
    }

    /**
     * Batched form of {@link #getById}: resolves every id via the secondary index, then fetches
     * the rows grouped by writer generation through {@link DocumentRowReader#executeRows} — one
     * backend call per file instead of one per document, amortizing session setup and page
     * decompression across the batch.
     *
     * <p>Ids are resolved in sorted order for terms-dictionary locality. The returned map is
     * keyed by id and contains an entry for every requested id (missing documents map to
     * {@link DocumentLookupResult#notFound}).
     */
    public Map<String, DocumentLookupResult> getByIds(List<String> ids, IndexReaderProvider.Reader reader, Index index) throws IOException {
        Map<String, DocumentLookupResult> results = new LinkedHashMap<>();
        if (ids.isEmpty()) {
            return results;
        }
        // Resolve in id order for terms-dict locality; output order is re-established at the end.
        List<String> sorted = new ArrayList<>(ids);
        Collections.sort(sorted);

        // generation -> resolved metadata needing a row fetch
        Map<Long, List<DocumentMetadata>> pendingByGeneration = new LinkedHashMap<>();
        Map<String, DocumentLookupResult> resolved = new LinkedHashMap<>();

        for (String id : sorted) {
            DocumentMetadata metadata = documentResolver.resolveMetadata(reader, id);
            if (metadata == null) {
                resolved.put(id, DocumentLookupResult.notFound(id));
                continue;
            }
            pendingByGeneration.computeIfAbsent(metadata.writerGeneration(), g -> new ArrayList<>()).add(metadata);
        }

        for (Map.Entry<Long, List<DocumentMetadata>> entry : pendingByGeneration.entrySet()) {
            long generation = entry.getKey();
            List<DocumentMetadata> metadatas = entry.getValue();
            WriterFileSet fileSet = reader.catalogSnapshot().findFileSet(executor.formatName(), generation);
            if (fileSet == null) {
                throw new IllegalStateException(
                    "Resolver located ids at writer generation [" + generation + "] but no matching file set was found"
                );
            }
            List<Long> rowIds = new ArrayList<>(metadatas.size());
            for (DocumentMetadata metadata : metadatas) {
                rowIds.add(metadata.rowId());
            }
            Map<Long, Map<String, Object>> rows = executor.executeRows(rowIds, fileSet);
            for (DocumentMetadata metadata : metadatas) {
                Map<String, Object> row = rows.get(metadata.rowId());
                if (row == null) {
                    throw new IllegalStateException(
                        "Resolver located id ["
                            + metadata.id()
                            + "] at writer generation ["
                            + generation
                            + "] rowId ["
                            + metadata.rowId()
                            + "] but backend returned no row"
                    );
                }
                resolved.put(metadata.id(), buildResultFromRow(metadata.id(), row));
            }
        }

        // Re-establish caller order.
        for (String id : ids) {
            results.put(id, resolved.get(id));
        }
        return results;
    }

    /** Engine/storage metadata columns excluded from {@code _source} and from coverage checks. */
    private static boolean isMetadataColumn(String name) {
        return METADATA_FIELDS.contains(name) || SeqNoFieldMapper.PRIMARY_TERM_NAME.equals(name) || DocumentInput.ROW_ID_FIELD.equals(name);
    }

    /**
     * Resolves version metadata without reconstructing {@code _source}. Uses resolver metadata when
     * available; legacy segments without these doc values fall back to a primary-store row lookup.
     */
    public DocumentLookupResult getVersionMetadata(String id, IndexReaderProvider.Reader reader, Index index) throws IOException {
        DocumentMetadata metadata = documentResolver.resolveMetadata(reader, id);
        if (metadata == null) {
            return DocumentLookupResult.notFound(id);
        }
        if (metadata.hasVersionMetadata()) {
            return new DocumentLookupResult(
                id,
                metadata.version(),
                true,
                null,
                metadata.seqNo(),
                metadata.primaryTerm(),
                Map.of(),
                Map.of()
            );
        }
        Map<String, Object> row = fetchRow(metadata, reader);
        long seqNo = extractLong(row, "_seq_no", SequenceNumbers.UNASSIGNED_SEQ_NO);
        long primaryTerm = extractLong(row, "_primary_term", SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
        long version = extractLong(row, "_version", Versions.NOT_FOUND);
        return new DocumentLookupResult(id, version, true, null, seqNo, primaryTerm, Map.of(), Map.of());
    }

    /** Fetches the raw row for an already-resolved document location. */
    private Map<String, Object> fetchRow(DocumentMetadata metadata, IndexReaderProvider.Reader reader) throws IOException {
        String id = metadata.id();
        WriterFileSet fileSet = reader.catalogSnapshot().findFileSet(executor.formatName(), metadata.writerGeneration());
        if (fileSet == null) {
            throw new IllegalStateException(
                "Resolver located id ["
                    + id
                    + "] at writer generation ["
                    + metadata.writerGeneration()
                    + "] but no matching file set was found"
            );
        }

        Map<String, Object> row = executor.executeSingleRow(metadata.rowId(), fileSet);
        if (row == null) {
            throw new IllegalStateException(
                "Resolver located id ["
                    + id
                    + "] at writer generation ["
                    + metadata.writerGeneration()
                    + "] rowId ["
                    + metadata.rowId()
                    + "] but backend returned no row"
            );
        }
        return row;
    }

    public List<DocumentLookupResult> getDocsAboveSeqNo(long fromSeqNoExclusive, IndexReaderProvider.Reader reader, Index index)
        throws IOException {
        List<WriterFileSet> fileSets = new ArrayList<>();
        for (Segment segment : reader.catalogSnapshot().getSegments()) {
            WriterFileSet fileSet = segment.dfGroupedSearchableFiles().get(executor.formatName());
            if (fileSet != null && !fileSet.files().isEmpty()) {
                fileSets.add(fileSet);
            }
        }
        List<DocumentLookupResult> results = new ArrayList<>();
        for (Map<String, Object> row : executor.executeRowsAboveSeqNo(fileSets, fromSeqNoExclusive)) {
            Object idVal = row.get("_id");
            if (idVal != null) {
                results.add(buildResultFromRow(idVal.toString(), row));
            }
        }
        return results;
    }

    /** Builds a DocumentLookupResult from a raw row, filtering metadata/internal fields out of {@code _source}. */
    private static DocumentLookupResult buildResultFromRow(String id, Map<String, Object> row) throws IOException {
        long seqNo = extractLong(row, "_seq_no", SequenceNumbers.UNASSIGNED_SEQ_NO);
        long primaryTerm = extractLong(row, "_primary_term", SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
        long version = extractLong(row, "_version", Versions.NOT_FOUND);

        Map<String, Object> filtered = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : row.entrySet()) {
            // Exclude registered metadata fields, plus the two columns that are not registered mappers:
            // _primary_term (a sub-column emitted by the _seq_no mapper) and __row_id__ (engine-internal).
            if (isMetadataColumn(e.getKey())) {
                continue;
            }
            filtered.put(e.getKey(), e.getValue());
        }

        BytesReference source;
        try (XContentBuilder xcb = XContentFactory.jsonBuilder()) {
            xcb.map(filtered);
            source = BytesReference.bytes(xcb);
        }

        return new DocumentLookupResult(id, version, true, source, seqNo, primaryTerm, Map.of(), Map.of());
    }

    public static long extractLong(Map<String, Object> row, String key, long fallback) {
        Object v = row.get(key);
        if (v == null) return fallback;
        if (v instanceof Number) return ((Number) v).longValue();
        try {
            return Long.parseLong(v.toString());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
