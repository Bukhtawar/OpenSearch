/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A storage-neutral description of how a field's column is laid out by a pluggable data format: an optional
 * value encoding followed by an optional block compression.
 *
 * <p>The mapping form is a single token or a list of tokens, with the encoding first:
 * <pre>
 *   "codec": "zstd(3)"
 *   "codec": ["delta", "zstd(3)"]
 *   "codec": ["dictionary", "lz4"]
 * </pre>
 *
 * <p>Encoding tokens are {@value #PLAIN}, {@value #DICTIONARY}, {@value #DELTA}, {@value #BYTE_SPLIT} and
 * {@value #RLE}. Compression tokens are {@value #ZSTD}, {@value #LZ4}, {@value #SNAPPY}, {@value #GZIP} and
 * {@value #NONE}; {@value #ZSTD} and {@value #GZIP} accept a level in parentheses. Tokens name the intent, not an
 * implementation: each data format translates them to its own physical encodings and rejects, at mapping time, any
 * token it cannot honour for the field's type.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FieldCodec {

    /** Encoding: store values as-is. */
    public static final String PLAIN = "plain";
    /** Encoding: dictionary-encode repeated values. */
    public static final String DICTIONARY = "dictionary";
    /** Encoding: store deltas between consecutive values (integers, timestamps) or shared prefixes (strings). */
    public static final String DELTA = "delta";
    /** Encoding: split values into per-byte streams so the compressor sees homogeneous bytes (floats, integers). */
    public static final String BYTE_SPLIT = "byte_split";
    /** Encoding: run-length encode (booleans). */
    public static final String RLE = "rle";

    /** Compression: Zstandard, optional level 1-22. */
    public static final String ZSTD = "zstd";
    /** Compression: LZ4. */
    public static final String LZ4 = "lz4";
    /** Compression: Snappy. */
    public static final String SNAPPY = "snappy";
    /** Compression: gzip, optional level 1-9. */
    public static final String GZIP = "gzip";
    /** Compression: none. */
    public static final String NONE = "none";

    public static final Set<String> ENCODINGS = Set.of(PLAIN, DICTIONARY, DELTA, BYTE_SPLIT, RLE);
    public static final Set<String> COMPRESSIONS = Set.of(ZSTD, LZ4, SNAPPY, GZIP, NONE);

    /** Compressions that accept a level, with the inclusive [min, max] range. */
    private static final Map<String, int[]> LEVEL_RANGES = Map.of(ZSTD, new int[] { 1, 22 }, GZIP, new int[] { 1, 9 });

    private static final Pattern TOKEN = Pattern.compile("^([a-z_0-9]+)(?:\\((\\d{1,3})\\))?$");

    @Nullable
    private final String encoding;
    @Nullable
    private final String compression;
    @Nullable
    private final Integer compressionLevel;

    private FieldCodec(@Nullable String encoding, @Nullable String compression, @Nullable Integer compressionLevel) {
        this.encoding = encoding;
        this.compression = compression;
        this.compressionLevel = compressionLevel;
    }

    /**
     * Parses the raw mapping value of a {@code codec} parameter: a string token or a list of one or two string tokens
     * (encoding first, then compression). Tokens are case-insensitive.
     *
     * @throws IllegalArgumentException if the value is malformed, names an unknown token, repeats a token class,
     *                                  orders compression before encoding, or carries a level the compression does
     *                                  not accept
     */
    public static FieldCodec parse(Object value) {
        List<String> tokens = tokens(value);
        String encoding = null;
        String compression = null;
        Integer level = null;
        for (String raw : tokens) {
            Matcher m = TOKEN.matcher(raw.trim().toLowerCase(Locale.ROOT));
            if (m.matches() == false) {
                throw new IllegalArgumentException(malformed(raw));
            }
            String name = m.group(1);
            String levelText = m.group(2);
            if (ENCODINGS.contains(name)) {
                if (encoding != null) {
                    throw new IllegalArgumentException("codec [" + raw + "] specifies a second encoding; only one is allowed");
                }
                if (compression != null) {
                    throw new IllegalArgumentException("codec encoding [" + raw + "] must precede the compression [" + compression + "]");
                }
                if (levelText != null) {
                    throw new IllegalArgumentException("codec encoding [" + raw + "] does not accept a level");
                }
                encoding = name;
            } else if (COMPRESSIONS.contains(name)) {
                if (compression != null) {
                    throw new IllegalArgumentException("codec [" + raw + "] specifies a second compression; only one is allowed");
                }
                compression = name;
                if (levelText != null) {
                    int[] range = LEVEL_RANGES.get(name);
                    if (range == null) {
                        throw new IllegalArgumentException("codec compression [" + name + "] does not accept a level");
                    }
                    int parsed = Integer.parseInt(levelText);
                    if (parsed < range[0] || parsed > range[1]) {
                        throw new IllegalArgumentException(
                            "codec compression [" + name + "] level must be between " + range[0] + " and " + range[1] + ", got " + parsed
                        );
                    }
                    level = parsed;
                }
            } else {
                throw new IllegalArgumentException(malformed(raw));
            }
        }
        return new FieldCodec(encoding, compression, level);
    }

    private static List<String> tokens(Object value) {
        if (value instanceof String s) {
            if (s.isBlank()) {
                throw new IllegalArgumentException("codec must not be empty");
            }
            return List.of(s);
        }
        if (value instanceof List<?> list) {
            if (list.isEmpty()) {
                throw new IllegalArgumentException("codec must not be empty");
            }
            if (list.size() > 2) {
                throw new IllegalArgumentException("codec accepts at most two tokens (an encoding and a compression), got " + list);
            }
            List<String> tokens = new ArrayList<>(list.size());
            for (Object o : list) {
                if (o instanceof String s && s.isBlank() == false) {
                    tokens.add(s);
                } else {
                    throw new IllegalArgumentException("codec tokens must be non-empty strings, got " + list);
                }
            }
            return tokens;
        }
        throw new IllegalArgumentException("codec must be a string or a list of strings, got [" + value + "]");
    }

    private static String malformed(String token) {
        return "unknown codec token ["
            + token
            + "]; encodings: "
            + ENCODINGS.stream().sorted().toList()
            + ", compressions: "
            + COMPRESSIONS.stream().sorted().toList()
            + " (zstd and gzip accept a level, e.g. zstd(3))";
    }

    /** The encoding token, or {@code null} when the codec does not constrain encoding. */
    @Nullable
    public String encoding() {
        return encoding;
    }

    /** The compression token, or {@code null} when the codec does not constrain compression. */
    @Nullable
    public String compression() {
        return compression;
    }

    /** The compression level, or {@code null} when unspecified or not applicable. */
    @Nullable
    public Integer compressionLevel() {
        return compressionLevel;
    }

    /** The canonical token list, encoding first. */
    public List<String> tokens() {
        List<String> tokens = new ArrayList<>(2);
        if (encoding != null) {
            tokens.add(encoding);
        }
        if (compression != null) {
            tokens.add(compressionLevel == null ? compression : compression + "(" + compressionLevel + ")");
        }
        return tokens;
    }

    /** The value written back to the mapping: a single string when one token is set, otherwise the token list. */
    public Object toMappingValue() {
        List<String> tokens = tokens();
        return tokens.size() == 1 ? tokens.get(0) : tokens;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof FieldCodec other) {
            return Objects.equals(encoding, other.encoding)
                && Objects.equals(compression, other.compression)
                && Objects.equals(compressionLevel, other.compressionLevel);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(encoding, compression, compressionLevel);
    }

    @Override
    public String toString() {
        return String.join(",", tokens());
    }
}
