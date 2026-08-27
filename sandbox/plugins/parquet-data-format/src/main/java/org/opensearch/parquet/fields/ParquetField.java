/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.util.List;
import java.util.Set;

/**
 * Abstract base class for Parquet field implementations that handle conversion
 * between OpenSearch field types and Apache Arrow vectors.
 */
public abstract class ParquetField {

    /**
     * Name of the child field inside a LIST column. Matches the parquet-rs convention
     * ({@code PARQUET_LIST_ELEMENT_NAME}) so the leaf path is {@code <field>.list.element}.
     */
    public static final String LIST_ELEMENT_NAME = "element";

    /** Creates a new ParquetField. */
    public ParquetField() {}

    /**
     * Writes the parsed field value into the appropriate vector in the managed VSR.
     * @param fieldType the mapped field type
     * @param managedVSR the managed vector schema root
     * @param parseValue the parsed value to write
     */
    protected abstract void addToGroup(MappedFieldType fieldType, ManagedVSR managedVSR, Object parseValue);

    /**
     * Writes a single parsed value at an explicit index in the given vector.
     * <p>
     * Scalar columns write at the row index, so {@link #addToGroup} can derive the position from
     * the VSR's row count. List columns write several values per row at positions in the child
     * vector that have nothing to do with the row number, so multi-value writes need this
     * index-explicit form instead.
     * <p>
     * Subclasses must override this to support being declared multi-valued; the default throws.
     * When overridden, {@link #addToGroup} should delegate to it so the scalar and list paths
     * share one value-coercion implementation.
     *
     * @param vector the target vector (the child data vector when writing into a list)
     * @param index the position to write at
     * @param parseValue the parsed value to write
     */
    protected void addToVector(FieldVector vector, int index, Object parseValue) {
        throw new UnsupportedOperationException(
            "Field type [" + getClass().getSimpleName() + "] does not support multi-valued (list) storage"
        );
    }

    /**
     * Returns whether this field can be stored as a Parquet LIST column, i.e. whether it
     * implements {@link #addToVector}.
     *
     * @return true if multi-valued storage is supported
     */
    public boolean supportsMultiValue() {
        return false;
    }

    /**
     * Builds the Arrow field describing this column, including any child fields.
     * <p>
     * When {@code multiValue} is true the result is a {@code LIST<element>} whose child carries
     * this field's element type, so the same {@link ParquetField} describes both shapes.
     *
     * @param name the Arrow field name
     * @param multiValue whether to wrap the element type in a list
     * @return the Arrow field
     */
    public final Field toArrowField(String name, boolean multiValue) {
        if (multiValue == false) {
            return new Field(name, getFieldType(), null);
        }
        if (supportsMultiValue() == false) {
            throw new IllegalArgumentException(
                "Field ["
                    + name
                    + "] cannot be stored as multi-valued: type ["
                    + getClass().getSimpleName()
                    + "] does not support list storage"
            );
        }
        // The element is always nullable: a null inside an array (e.g. ["a", null]) is a legal
        // document even when the column itself is declared non-nullable.
        Field element = new Field(LIST_ELEMENT_NAME, FieldType.nullable(getArrowType()), null);
        return new Field(name, FieldType.nullable(ArrowType.List.INSTANCE), List.of(element));
    }

    /**
     * Creates and processes a field entry. Throws if vector not present in VSR.
     * <p>
     * Adaptive multi-value encoding: a declared LIST column may still be accumulating as an
     * optimistic scalar ({@link ManagedVSR#isPromotable}). A singleton value writes through the
     * scalar fast path — no offset bookkeeping, no list dispatch. The first document that
     * carries several values promotes the column in place ({@link ManagedVSR#promoteToList})
     * and writes through the list path from then on. This is the write-side analogue of
     * Lucene's flush-time SORTED/SORTED_SET choice: singleton is an encoding optimization,
     * never a schema shape.
     *
     * @param fieldType the mapped field type
     * @param managedVSR the managed vector schema root
     * @param parseValue the parsed value to write
     */
    public final void createField(MappedFieldType fieldType, ManagedVSR managedVSR, Object parseValue) {
        assert fieldType != null : "MappedFieldType cannot be null";
        assert managedVSR != null : "ManagedVSR cannot be null";
        FieldVector vector = managedVSR.getVector(fieldType.name());
        if (vector instanceof ListVector listVector) {
            writeList(fieldType, managedVSR, listVector, parseValue);
            return;
        }
        if (managedVSR.isPromotable(fieldType.name())) {
            List<?> values = parseValue instanceof List<?> list ? list : (parseValue == null ? List.of() : List.of(parseValue));
            if (values.size() <= 1) {
                // Singleton fast path: write the element as a plain scalar. A null or empty
                // entry leaves the slot unset, which reads back as null — the same document
                // shape a null list represents after promotion.
                Object single = values.isEmpty() ? null : values.get(0);
                if (single != null) {
                    addToVector(vector, managedVSR.getRowCount(), single);
                }
                return;
            }
            ListVector promoted = (ListVector) managedVSR.promoteToList(fieldType.name());
            writeList(fieldType, managedVSR, promoted, parseValue);
            return;
        }
        addToGroup(fieldType, managedVSR, parseValue);
    }

    /**
     * Writes all values collected for one document into a list column at the current row.
     * <p>
     * A null {@code parseValue} is written as a null list, which is how an absent field is
     * represented. An empty list is written as a zero-length, non-null list, preserving the
     * distinction between {@code "tags": []} and no {@code tags} at all.
     */
    private void writeList(MappedFieldType fieldType, ManagedVSR managedVSR, ListVector listVector, Object parseValue) {
        int row = managedVSR.getRowCount();
        if (parseValue == null) {
            listVector.setNull(row);
            return;
        }
        List<?> values = parseValue instanceof List<?> list ? list : List.of(parseValue);
        int start = listVector.startNewValue(row);
        FieldVector dataVector = listVector.getDataVector();
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (value == null) {
                dataVector.setNull(start + i);
            } else {
                addToVector(dataVector, start + i, value);
            }
        }
        listVector.endValue(row, values.size());
    }

    /**
     * Returns the set of capabilities supported by this field type.
     * Subclasses may override to declare different capabilities.
     *
     * @return set of supported {@link FieldTypeCapabilities.Capability}
     */
    public Set<FieldTypeCapabilities.Capability> supportedCapabilities() {
        return Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.BLOOM_FILTER);
    }

    /** Returns the Arrow type for this field. */
    public abstract ArrowType getArrowType();

    /** Returns the Arrow field type with nullability metadata. */
    public abstract FieldType getFieldType();
}
