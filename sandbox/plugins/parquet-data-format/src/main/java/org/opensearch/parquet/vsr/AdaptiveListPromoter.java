/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.BitSet;

/**
 * Promotes an optimistically-scalar column accumulator to its declared {@code LIST} shape.
 *
 * <p>POC (adaptive multi-value encoding): a column whose <em>declared</em> Arrow schema is
 * {@code LIST&lt;element&gt;} starts life in each VSR as a plain scalar vector of the element
 * type — the common all-singleton case then pays no per-value list-writer dispatch and no
 * offset bookkeeping during accumulation. The first document that actually carries several
 * values for the column triggers {@link #promote}, and any column still scalar when the VSR
 * freezes is promoted wholesale so every batch exported to the native writer matches the
 * declared LIST schema. The on-disk Parquet shape is therefore always LIST — identical to the
 * non-adaptive path — which keeps the read side and the merger untouched.
 *
 * <p>This mirrors Lucene's doc-values structure inverted: Lucene buffers multi-value-capable
 * ({@code SORTED_SET}) and silently downgrades to {@code SORTED} at flush when every doc had
 * at most one value; here we buffer scalar and upgrade at first array (or at freeze), with the
 * file-level encoding decision pinned to LIST so the relational type exposed to the query
 * layer never changes.
 *
 * <p>Promotion is cheap: the scalar vector's buffers are moved (not copied) into the list's
 * element vector via a {@link org.apache.arrow.vector.util.TransferPair}, and the list level is
 * reconstructed as <em>identity offsets</em> — row {@code i}'s list is the child range
 * {@code [i, i+1)} — because a scalar vector keeps one child slot per row, nulls included.
 * A null scalar slot becomes a null list (top-level validity cleared); the child slot beneath
 * it is garbage under a null mask, which the Arrow spec permits.
 */
final class AdaptiveListPromoter {

    private AdaptiveListPromoter() {}

    /**
     * Whether a declared field can be seeded as a scalar and promoted later: a
     * single-child LIST column.
     */
    static boolean isAdaptable(Field field) {
        return field.getType() instanceof ArrowType.List && field.getChildren().size() == 1;
    }

    /**
     * Returns the scalar seed field for a declared LIST column: the element type carrying
     * the column's name, so the accumulator writes exactly as a scalar column would.
     */
    static Field scalarSeed(Field declaredListField) {
        Field element = declaredListField.getChildren().get(0);
        return new Field(declaredListField.getName(), element.getFieldType(), element.getChildren());
    }

    /**
     * Builds the declared {@link ListVector} from a scalar accumulator holding {@code rowCount}
     * rows. The scalar's buffers are transferred (zero-copy) into the element vector; the list
     * level is synthesized with identity offsets and the scalar's validity. The scalar vector is
     * left cleared; the caller is responsible for closing it.
     *
     * @param scalar the scalar accumulator (one child slot per row, nulls included)
     * @param declaredListField the declared LIST field for the column
     * @param allocator allocator for the new list-level buffers
     * @param rowCount rows currently held
     * @return the promoted list vector, positioned so subsequent
     *         {@code startNewValue(rowCount)}/{@code endValue} calls continue correctly
     */
    static ListVector promote(FieldVector scalar, Field declaredListField, BufferAllocator allocator, int rowCount) {
        // A column added by a late mapping update that no document ever wrote reaches
        // freeze-time canonicalization with under-allocated buffers (value count 0 while the
        // VSR already holds rows). setValueCount allocates the validity/offset buffers so the
        // null scan and the buffer transfer below are safe; every slot reads back null.
        if (scalar.getValueCount() < rowCount) {
            scalar.setValueCount(rowCount);
        }
        // Capture validity before the transfer clears the scalar.
        BitSet nonNull = new BitSet(rowCount);
        for (int i = 0; i < rowCount; i++) {
            if (scalar.isNull(i) == false) {
                nonNull.set(i);
            }
        }
        ListVector list = (ListVector) declaredListField.createVector(allocator);
        boolean success = false;
        try {
            // +1 so the offset buffer covers indices 0..rowCount inclusive.
            list.setInitialCapacity(rowCount + 1);
            list.allocateNew();
            // Move the scalar's data into the element vector: buffers change owner, no copy.
            scalar.makeTransferPair(list.getDataVector()).transfer();

            // Identity offsets: row i's list is child range [i, i+1). Monotonic as required;
            // null rows keep the range but are masked by the cleared top-level validity bit.
            ArrowBuf offsets = list.getOffsetBuffer();
            for (int i = 0; i <= rowCount; i++) {
                offsets.setInt((long) i * Integer.BYTES, i);
            }
            ArrowBuf validity = list.getValidityBuffer();
            for (int i = nonNull.nextSetBit(0); i >= 0; i = nonNull.nextSetBit(i + 1)) {
                BitVectorHelper.setBit(validity, i);
            }
            // lastSet = rowCount - 1 marks offsets as written through index rowCount, so the
            // next startNewValue(rowCount) neither refills nor clobbers what we synthesized.
            list.setLastSet(rowCount - 1);
            list.setValueCount(rowCount);
            success = true;
            return list;
        } finally {
            if (success == false) {
                list.close();
            }
        }
    }
}
