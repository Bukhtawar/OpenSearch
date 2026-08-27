/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import java.util.HashMap;
import java.util.Map;

/**
 * Per-file shape latch for adaptively-encoded (declared-LIST) columns.
 *
 * <p>Lazy LIST materialization: a Parquet file's schema is fixed the moment its first batch
 * is handed to the native writer, so the shape each adaptive column takes <em>in this file</em>
 * is decided exactly once — from the column's physical shape in that first frozen batch — and
 * every later batch of the same file must conform:
 *
 * <ul>
 *   <li>{@code UNDECIDED} (writer not yet initialized): the column may promote freely, and a
 *       freeze exports whatever physical shape the column currently has. All-singleton batches
 *       freeze as plain scalars — a file that never sees an array is a scalar file, end to end.</li>
 *   <li>{@code SCALAR} (first batch froze scalar): the file can never hold an array. A document
 *       carrying several values is rejected recoverably (the caller rolls it back and retries it
 *       into a new writer generation, whose fresh latch starts {@code UNDECIDED}).</li>
 *   <li>{@code LIST} (first batch froze promoted): later all-singleton batches are canonicalized
 *       to LIST at freeze so every batch matches the file schema.</li>
 * </ul>
 *
 * <p>The latch is scoped to one {@link VSRManager} — i.e. one output file — so the decision
 * resets naturally at every flush/generation boundary. Not thread-safe; confined to the ingest
 * thread like the rest of the VSR layer.
 */
final class AdaptiveShapePolicy {

    /** The shape a column is locked to in the current file. */
    enum Shape {
        /** Native writer not initialized yet: shape not locked, promotion allowed. */
        UNDECIDED,
        /** File schema holds this column as a plain scalar: promotion forbidden. */
        SCALAR,
        /** File schema holds this column as LIST: scalar batches canonicalize at freeze. */
        LIST
    }

    private final Map<String, Shape> locked = new HashMap<>();

    Shape shapeOf(String fieldName) {
        return locked.getOrDefault(fieldName, Shape.UNDECIDED);
    }

    /** Whether the column may promote scalar → LIST right now. */
    boolean promotionAllowed(String fieldName) {
        return shapeOf(fieldName) != Shape.SCALAR;
    }

    /** Whether a still-scalar column must be canonicalized to LIST when its batch freezes. */
    boolean canonicalizeAtFreeze(String fieldName) {
        return shapeOf(fieldName) == Shape.LIST;
    }

    /** Locks a column's file shape; first lock wins (the file schema cannot change). */
    void lock(String fieldName, boolean isList) {
        locked.putIfAbsent(fieldName, isList ? Shape.LIST : Shape.SCALAR);
    }
}
