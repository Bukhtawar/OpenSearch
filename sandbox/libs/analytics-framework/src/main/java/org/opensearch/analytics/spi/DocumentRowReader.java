/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Backend-specific execution contract for reading document rows. Backends implement this
 * to perform the actual storage read (e.g., DataFusion native parquet scan) from a file set
 * that the Core layer has already resolved from the catalog snapshot.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DocumentRowReader {

    /**
     * The storage format this backend reads (e.g. {@code "parquet"}). Lets the Core layer
     * resolve the backend's candidate {@link WriterFileSet} from the catalog snapshot.
     */
    String formatName();

    /**
     * Fetch a single row at the given offset from the pre-resolved file set.
     *
     * @param rowId the row offset to fetch
     * @param fileSet the file set to read from
     * @return the row as a field-name → value map, or null if not found
     */
    Map<String, Object> executeSingleRow(long rowId, WriterFileSet fileSet) throws IOException;

    /**
     * Fetch multiple rows from ONE pre-resolved file set in a single backend call, amortizing
     * per-call session setup, page decompression, and transport across the batch. Row ids must
     * be non-negative; implementations may require nothing about their order (this default and
     * the native implementation both sort internally as needed).
     *
     * <p>The default delegates to per-row {@link #executeSingleRow} so existing backends keep
     * working; backends with a cheaper bulk path should override.
     *
     * @param rowIds the row offsets to fetch, all within {@code fileSet}
     * @param fileSet the file set to read from
     * @return map from row id to its field-name → value map; ids whose row was not found are absent
     */
    default Map<Long, Map<String, Object>> executeRows(List<Long> rowIds, WriterFileSet fileSet) throws IOException {
        Map<Long, Map<String, Object>> out = new LinkedHashMap<>();
        for (Long rowId : rowIds) {
            Map<String, Object> row = executeSingleRow(rowId, fileSet);
            if (row != null) {
                out.put(rowId, row);
            }
        }
        return out;
    }

    /**
     * Fetch all rows with {@code _seq_no > fromSeqNoExclusive} from the Core-resolved file sets
     * (one per segment for this backend's format).
     *
     * @param fileSets the file sets to scan
     * @param fromSeqNoExclusive the exclusive lower bound on {@code _seq_no}
     */
    List<Map<String, Object>> executeRowsAboveSeqNo(List<WriterFileSet> fileSets, long fromSeqNoExclusive) throws IOException;

    /**
     * The dotted leaf column paths of the given file set's schema (e.g. {@code user.name}),
     * including metadata columns. Used for update coverage checks: when an update document's
     * covering paths span every non-metadata column, the stored row need not be read at all.
     * File sets are immutable, so implementations should cache per file.
     *
     * @return the column paths, or {@code null} when the backend cannot provide them (callers
     *         must then fall back to reading the row)
     */
    default Set<String> columnPaths(WriterFileSet fileSet) throws IOException {
        return null;
    }
}
