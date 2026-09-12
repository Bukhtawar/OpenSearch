/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParametrizedFieldMapper.SideEffectParameter;

import java.util.function.Consumer;

/**
 * Factories for the storage-neutral field mapping parameters a {@link DataFormatPlugin} may contribute through
 * {@link DataFormatPlugin#getPluginMappingParameters}. Parameters built here share one vocabulary across formats;
 * each format supplies its own validator and translates the resolved value to its physical layout.
 *
 * <p>These parameters only exist on indices that use a pluggable data format: the registry contributes them per
 * index, so a plain Lucene index rejects them as unknown.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FieldStorageParameters {

    /** Mapping parameter holding a {@link FieldCodec}: how the field's column is encoded and compressed. */
    public static final String CODEC = "codec";

    /** Boolean mapping parameter requesting a per-column bloom filter for equality lookups. */
    public static final String BLOOM_FILTER = "bloom_filter";

    private FieldStorageParameters() {}

    /**
     * Creates a {@value #CODEC} parameter. Absent by default; not updateable, since files already written keep their
     * layout. The given validator runs after parsing and should reject codecs the format cannot honour for the
     * field's type.
     *
     * @param validator format-specific check applied to the parsed codec
     */
    public static SideEffectParameter<FieldCodec> codec(Consumer<FieldCodec> validator) {
        return SideEffectParameter.create(CODEC, false, (FieldCodec) null, (name, context, value) -> {
            FieldCodec codec = FieldCodec.parse(value);
            validator.accept(codec);
            return codec;
        }, (builder, codec) -> {}, FieldCodec::toMappingValue);
    }

    /** Creates a {@value #BLOOM_FILTER} parameter, defaulting to {@code false} and not updateable. */
    public static ParametrizedFieldMapper.Parameter<Boolean> bloomFilter() {
        return SideEffectParameter.boolParam(BLOOM_FILTER, false, false, (builder, enabled) -> {});
    }
}
