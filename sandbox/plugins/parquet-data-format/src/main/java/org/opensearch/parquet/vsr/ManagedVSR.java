/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.nativebridge.spi.ArrowExport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Managed wrapper around an Apache Arrow {@link VectorSchemaRoot} with strict lifecycle enforcement.
 *
 * <p>Each instance follows the state machine: {@code ACTIVE → FROZEN → CLOSED}.
 * <ul>
 *   <li><strong>ACTIVE</strong> — Vectors are writable; row count can be incremented.</li>
 *   <li><strong>FROZEN</strong> — Read-only; data can be exported to the native writer via
 *       {@link #exportToArrow()} using the Arrow C Data Interface.</li>
 *   <li><strong>CLOSED</strong> — All Arrow resources (vectors and child allocator) are released.</li>
 * </ul>
 *
 * <p>State transitions are enforced: writing to a frozen VSR or closing an active VSR
 * (without freezing first) throws {@link IllegalStateException}.
 *
 * <p>This class is NOT Thread-Safe. External synchronization is required
 * if instances are shared across threads.
 */
public class ManagedVSR implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(ManagedVSR.class);

    private final String id;
    private VectorSchemaRoot vsr;
    private final BufferAllocator allocator;
    private final AtomicReference<VSRState> state = new AtomicReference<>(VSRState.ACTIVE);
    private final Map<String, FieldVector> fields = new HashMap<>();
    /**
     * Declared LIST fields currently accumulating as scalar vectors (adaptive encoding).
     * Keyed by field name; the value is the declared LIST field to promote to. Emptied as
     * columns are promoted — any survivors are promoted wholesale in {@link #moveToFrozen}.
     */
    private final Map<String, Field> pendingListPromotions = new HashMap<>();
    /** The declared (logical) fields, LIST-shaped even while accumulating as scalar. */
    private final List<Field> declaredFields = new ArrayList<>();

    /**
     * Creates a new ManagedVSR.
     *
     * @param id unique identifier for this VSR
     * @param schema Arrow schema defining the vector structure
     * @param allocator buffer allocator for Arrow memory
     */
    public ManagedVSR(String id, Schema schema, BufferAllocator allocator) {
        this.id = id;
        // POC (adaptive multi-value encoding): a declared LIST column starts as a scalar vector
        // of its element type — the all-singleton common case pays no offset bookkeeping while
        // accumulating. The column is promoted to its declared LIST shape by the first document
        // that carries several values (ParquetField.createField) or at freeze time, so the
        // schema exported to the native writer is always the declared one.
        List<Field> physicalFields = new ArrayList<>(schema.getFields().size());
        for (Field field : schema.getFields()) {
            declaredFields.add(field);
            if (AdaptiveListPromoter.isAdaptable(field)) {
                pendingListPromotions.put(field.getName(), field);
                physicalFields.add(AdaptiveListPromoter.scalarSeed(field));
            } else {
                physicalFields.add(field);
            }
        }
        this.vsr = VectorSchemaRoot.create(new Schema(physicalFields), allocator);
        this.allocator = allocator;
        for (Field field : vsr.getSchema().getFields()) {
            fields.put(field.getName(), vsr.getVector(field));
        }
    }

    /** Returns the current row count. */
    public int getRowCount() {
        return vsr.getRowCount();
    }

    /**
     * Sets the row count.
     *
     * @param rowCount the new row count
     */
    public void setRowCount(int rowCount) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot modify VSR in state: " + state.get());
        }
        int previous = vsr.getRowCount();
        // Rollback: rows in [rowCount, previous) were written by rejected documents. Their
        // validity bits (and, for list columns, lastSet/offset state) were already set by the
        // write path and would otherwise leak into the next document that reuses the slot —
        // a document that leaves the field ABSENT would read back the rejected document's
        // values as a ghost. Clear the trimmed slots so a rolled-back slot is
        // indistinguishable from a never-written one.
        if (rowCount < previous) {
            for (FieldVector vector : fields.values()) {
                for (int row = previous - 1; row >= rowCount; row--) {
                    clearSlot(vector, row);
                }
            }
        }
        vsr.setRowCount(rowCount);
    }

    /**
     * Clears one row slot of a vector after rollback: the validity bit is cleared so the slot
     * reads as null again. For a {@link ListVector} the offset chain and {@code lastSet} are
     * rewound too, so the next write at this row starts its child range where the rolled-back
     * row started (stale child bytes beyond it are unreachable — offsets are the only access
     * path into a list's child vector).
     */
    private static void clearSlot(FieldVector vector, int row) {
        if (vector instanceof ListVector listVector) {
            listVector.setNull(row);
            ArrowBuf offsets = listVector.getOffsetBuffer();
            offsets.setInt((long) (row + 1) * Integer.BYTES, offsets.getInt((long) row * Integer.BYTES));
            if (listVector.getLastSet() >= row) {
                listVector.setLastSet(row - 1);
            }
        } else {
            vector.setNull(row);
        }
    }

    /**
     * Returns the vector for the given field name, or null if not found.
     * @param fieldName the field name
     * @return the field vector, or null
     */
    public FieldVector getVector(String fieldName) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot access vector in VSR state: " + state.get());
        }
        return fields.get(fieldName);
    }

    /**
     * Whether the column is still accumulating as an optimistic scalar and can be promoted
     * to its declared LIST shape on demand.
     */
    public boolean isPromotable(String fieldName) {
        return pendingListPromotions.containsKey(fieldName);
    }

    /**
     * Promotes a column from its optimistic scalar accumulator to the declared LIST shape,
     * carrying the rows written so far as single-element lists (identity offsets, zero-copy
     * buffer transfer). Called by the write path when a document first supplies several
     * values for the column. Only allowed in ACTIVE state.
     *
     * @param fieldName the column to promote
     * @return the promoted list vector, ready for {@code startNewValue(getRowCount())}
     */
    public FieldVector promoteToList(String fieldName) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot promote field in VSR state: " + state.get());
        }
        Field declared = pendingListPromotions.remove(fieldName);
        if (declared == null) {
            throw new IllegalStateException("Field [" + fieldName + "] is not pending list promotion");
        }
        FieldVector scalar = fields.get(fieldName);
        FieldVector promoted = AdaptiveListPromoter.promote(scalar, declared, allocator, vsr.getRowCount());
        replaceVector(fieldName, scalar, promoted, declared);
        logger.debug("Promoted column [{}] scalar -> LIST at row {} in VSR {}", fieldName, vsr.getRowCount(), id);
        return promoted;
    }

    /**
     * Swaps a column's vector in the VSR, closing the old vector. The schema entry for the
     * column is the <em>declared</em> field, not {@code newVector.getField()}: Arrow renames a
     * list vector's child to {@code $data$} internally, and the VSR schema is what
     * {@link #exportSchema()} hands to the native writer — it must keep the declared element
     * name or the Parquet leaf path would change.
     */
    private void replaceVector(String fieldName, FieldVector oldVector, FieldVector newVector, Field declaredField) {
        int rowCount = vsr.getRowCount();
        List<Field> oldSchemaFields = vsr.getSchema().getFields();
        List<Field> newFields = new ArrayList<>(oldSchemaFields.size());
        List<FieldVector> newVectors = new ArrayList<>(oldSchemaFields.size());
        List<FieldVector> oldVectors = vsr.getFieldVectors();
        for (int i = 0; i < oldVectors.size(); i++) {
            FieldVector vector = oldVectors.get(i);
            if (vector == oldVector) {
                newVectors.add(newVector);
                newFields.add(declaredField);
            } else {
                newVectors.add(vector);
                newFields.add(oldSchemaFields.get(i));
            }
        }
        vsr = new VectorSchemaRoot(newFields, newVectors, rowCount);
        fields.put(fieldName, newVector);
        oldVector.close();
    }

    /** Transitions this VSR from ACTIVE to FROZEN state. */
    public void moveToFrozen() {
        if (state.get() == VSRState.ACTIVE && pendingListPromotions.isEmpty() == false) {
            // Canonicalize: every declared-LIST column still scalar becomes single-element
            // lists now, so the exported batch always matches the declared schema and the
            // native writer sees one file shape regardless of what the data looked like.
            // (The Lucene analogue is the flush-time SORTED/SORTED_SET encoding decision —
            // ours is pinned to LIST so the file schema, read path and merger stay uniform.)
            for (String fieldName : List.copyOf(pendingListPromotions.keySet())) {
                promoteToList(fieldName);
            }
        }
        if (state.compareAndSet(VSRState.ACTIVE, VSRState.FROZEN) == false) {
            throw new IllegalStateException("Cannot freeze VSR " + id + ": expected ACTIVE but was " + state.get());
        }
        logger.debug("State transition: ACTIVE -> FROZEN for VSR {}", id);
    }

    /**
     * Exports this VSR to Arrow C Data Interface for native handoff.
     * Only allowed when VSR is FROZEN.
     */
    public ArrowExport exportToArrow() {
        if (state.get() != VSRState.FROZEN) {
            throw new IllegalStateException("Cannot export VSR in state: " + state.get() + ". Must be FROZEN.");
        }
        ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportVectorSchemaRoot(allocator, vsr, null, arrowArray, arrowSchema);
        return new ArrowExport(arrowArray, arrowSchema);
    }

    /**
     * Exports only the schema to Arrow C Data Interface.
     */
    public ArrowSchema exportSchema() {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, vsr.getSchema(), null, arrowSchema);
        return arrowSchema;
    }

    /**
     * Returns the current lifecycle state.
     *
     * @return the VSR state
     */
    public VSRState getState() {
        return state.get();
    }

    /**
     * Dynamically adds a new field to this VSR. Creates the vector using the internal
     * allocator and appends it to the schema. Only allowed in ACTIVE state.
     * <p>
     * A declared LIST field is seeded as an optimistic scalar of its element type, exactly
     * like construction-time fields; see the constructor for the adaptive-encoding contract.
     *
     * @param field the Arrow field descriptor
     */
    public void addFieldVector(Field field) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot add field to VSR in state: " + state.get());
        }
        declaredFields.add(field);
        Field physicalField = field;
        if (AdaptiveListPromoter.isAdaptable(field)) {
            pendingListPromotions.put(field.getName(), field);
            physicalField = AdaptiveListPromoter.scalarSeed(field);
        }
        FieldVector vector = physicalField.createVector(allocator);
        List<FieldVector> vectors = new ArrayList<>(vsr.getFieldVectors());
        vectors.add(vector);
        List<Field> newFields = new ArrayList<>(vsr.getSchema().getFields());
        newFields.add(physicalField);
        int rowCount = vsr.getRowCount();
        vsr = new VectorSchemaRoot(newFields, vectors, rowCount);
        fields.put(physicalField.getName(), vector);
    }

    /**
     * Returns the current Arrow schema of this VSR.
     *
     * @return the schema
     */
    public Schema getSchema() {
        return vsr.getSchema();
    }

    /**
     * Returns the declared (logical) schema: declared-LIST columns appear as LIST here even
     * while their accumulator is still an optimistic scalar. This is the schema new VSRs
     * must be created from and the shape every frozen batch is canonicalized to.
     *
     * @return the declared schema
     */
    public Schema getDeclaredSchema() {
        return new Schema(declaredFields);
    }

    /**
     * Returns the unique identifier.
     *
     * @return the VSR id
     */
    public String getId() {
        return id;
    }

    @Override
    public void close() {
        if (state.get() == VSRState.CLOSED) {
            return;
        }
        if (state.get() == VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot close VSR " + id + ": must freeze first");
        }
        if (state.compareAndSet(VSRState.FROZEN, VSRState.CLOSED) == false) {
            throw new IllegalStateException("Expected VSR to be FROZEN but was " + state.get());
        }
        logger.debug("State transition: FROZEN -> CLOSED for VSR {}", id);
        if (vsr != null) {
            vsr.close();
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    @Override
    public String toString() {
        return "ManagedVSR{id='" + id + "', state=" + state.get() + ", rows=" + getRowCount() + "}";
    }
}
