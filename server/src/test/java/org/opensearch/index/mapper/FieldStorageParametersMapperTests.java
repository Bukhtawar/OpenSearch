/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.dataformat.FieldCodec;
import org.opensearch.index.engine.dataformat.FieldStorageParameters;
import org.opensearch.index.mapper.ParametrizedFieldMapper.Parameter;
import org.opensearch.index.mapper.ParametrizedFieldMapper.SideEffectParameter;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Verifies that the number and date mappers carry plugin-contributed storage parameters ({@code codec},
 * {@code bloom_filter}) through build, serialization, re-initialisation and merge, the same way the keyword and
 * text mappers already do.
 */
public class FieldStorageParametersMapperTests extends OpenSearchTestCase {

    private static final Mapper.BuilderContext CONTEXT = new Mapper.BuilderContext(Settings.EMPTY, new ContentPath(0));

    public void testNumberMapperCarriesStorageParameters() throws IOException {
        FieldCodec codec = FieldCodec.parse(List.of("delta", "zstd(3)"));
        NumberFieldMapper mapper = numberMapper("count", codec, true);

        assertEquals(codec, mapper.mappingPluginParameterValues().get(FieldStorageParameters.CODEC));
        assertEquals(Boolean.TRUE, mapper.mappingPluginParameterValues().get(FieldStorageParameters.BLOOM_FILTER));

        String json = toJson(mapper);
        assertThat(json, containsString("\"codec\":[\"delta\",\"zstd(3)\"]"));
        assertThat(json, containsString("\"bloom_filter\":true"));

        // The merge builder reads the values back from the built mapper.
        NumberFieldMapper.Builder merge = (NumberFieldMapper.Builder) mapper.getMergeBuilder();
        assertEquals(codec, parameter(merge, FieldStorageParameters.CODEC).getValue());
        assertEquals(Boolean.TRUE, parameter(merge, FieldStorageParameters.BLOOM_FILTER).getValue());
    }

    public void testDateMapperCarriesStorageParameters() throws IOException {
        FieldCodec codec = FieldCodec.parse("zstd");
        DateFieldMapper mapper = dateMapper("@timestamp", codec, false);

        assertEquals(codec, mapper.mappingPluginParameterValues().get(FieldStorageParameters.CODEC));
        String json = toJson(mapper);
        assertThat(json, containsString("\"codec\":\"zstd\""));
        // bloom_filter=false is the default and is not serialized.
        assertThat(json, not(containsString("bloom_filter")));

        DateFieldMapper.Builder merge = (DateFieldMapper.Builder) mapper.getMergeBuilder();
        assertEquals(codec, parameter(merge, FieldStorageParameters.CODEC).getValue());
    }

    public void testAbsentCodecLeavesNoTraceInMapping() throws IOException {
        NumberFieldMapper mapper = numberMapper("count", null, false);
        assertNull(mapper.mappingPluginParameterValues().get(FieldStorageParameters.CODEC));
        String json = toJson(mapper);
        assertThat(json, not(containsString("codec")));
        assertThat(json, not(containsString("bloom_filter")));
    }

    public void testCodecIsNotUpdateable() {
        NumberFieldMapper existing = numberMapper("count", FieldCodec.parse("zstd(3)"), false);
        NumberFieldMapper changed = numberMapper("count", FieldCodec.parse("lz4"), false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> existing.merge(changed));
        assertThat(e.getMessage(), containsString("Cannot update parameter [codec] from [zstd(3)] to [lz4]"));

        // Merging an identical codec is a no-op rather than a conflict.
        NumberFieldMapper same = numberMapper("count", FieldCodec.parse("zstd(3)"), false);
        assertEquals(FieldCodec.parse("zstd(3)"), existing.merge(same).mappingPluginParameterValues().get(FieldStorageParameters.CODEC));
    }

    public void testCodecValidatorIsAppliedByTheFactory() {
        // The plugin-supplied validator is part of the parameter's parser; a rejecting validator surfaces its message.
        SideEffectParameter<FieldCodec> rejecting = FieldStorageParameters.codec(
            c -> { throw new IllegalArgumentException("rejected by format"); }
        );
        SideEffectParameter<FieldCodec> accepting = FieldStorageParameters.codec(c -> {});

        Builder rejectingBuilder = new Builder("f", List.of(rejecting));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> rejectingBuilder.parse("f", null, new java.util.HashMap<>(java.util.Map.of(FieldStorageParameters.CODEC, "rle")))
        );
        assertThat(e.getMessage(), containsString("rejected by format"));

        Builder acceptingBuilder = new Builder("f", List.of(accepting));
        acceptingBuilder.parse(
            "f",
            null,
            new java.util.HashMap<>(java.util.Map.of(FieldStorageParameters.CODEC, List.of("delta", "zstd")))
        );
        assertEquals(FieldCodec.parse(List.of("delta", "zstd")), accepting.getValue());
    }

    /** Minimal builder exposing {@link ParametrizedFieldMapper.Builder#parse} for the validator test. */
    private static class Builder extends ParametrizedFieldMapper.Builder {
        Builder(String name, List<Parameter<?>> pluginParameters) {
            super(name);
            setPluginMappingParameters(pluginParameters);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.copyOf(pluginMappingParameters());
        }

        @Override
        public ParametrizedFieldMapper build(Mapper.BuilderContext context) {
            throw new UnsupportedOperationException();
        }
    }

    // --- helpers ---

    private static NumberFieldMapper numberMapper(String name, FieldCodec codec, boolean bloom) {
        SideEffectParameter<FieldCodec> codecParam = FieldStorageParameters.codec(c -> {});
        Parameter<Boolean> bloomParam = FieldStorageParameters.bloomFilter();
        NumberFieldMapper.Builder builder = new NumberFieldMapper.Builder(
            name,
            NumberFieldMapper.NumberType.LONG,
            false,
            false,
            List.of(codecParam, bloomParam)
        );
        if (codec != null) {
            codecParam.setValue(codec);
        }
        if (bloom) {
            bloomParam.setValue(true);
        }
        return builder.build(CONTEXT);
    }

    private static DateFieldMapper dateMapper(String name, FieldCodec codec, boolean bloom) {
        SideEffectParameter<FieldCodec> codecParam = FieldStorageParameters.codec(c -> {});
        Parameter<Boolean> bloomParam = FieldStorageParameters.bloomFilter();
        DateFieldMapper.Builder builder = new DateFieldMapper.Builder(
            name,
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            false,
            Version.CURRENT,
            List.of(codecParam, bloomParam)
        );
        if (codec != null) {
            codecParam.setValue(codec);
        }
        if (bloom) {
            bloomParam.setValue(true);
        }
        return builder.build(CONTEXT);
    }

    private static Parameter<?> parameter(ParametrizedFieldMapper.Builder builder, String name) {
        for (Parameter<?> parameter : builder.getParameters()) {
            if (parameter.name.equals(name)) {
                return parameter;
            }
        }
        throw new AssertionError("no parameter [" + name + "]");
    }

    private static String toJson(ParametrizedFieldMapper mapper) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.toString();
    }
}
