/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.deephaven.json.TestHelper.parse;

public class ObjectArrayOptionsTest {

    public static final ObjectOptions FOO_BAR_BAZ = ObjectOptions.builder()
            .putFields("foo", StringOptions.standard())
            .putFields("bar", IntOptions.standard())
            .putFields("baz", ObjectOptions.builder()
                    .putFields("bip", LongOptions.standard())
                    .putFields("bop", DoubleOptions.standard())
                    .build())
            .build();

    public static final ObjectOptions FOO_BAR_BAZ_ARRAY = ObjectOptions.builder()
            .putFields("foo", StringOptions.standard())
            .putFields("bar", IntOptions.standard())
            .putFields("baz", ObjectOptions.builder()
                    .putFields("bip", LongOptions.standard())
                    .putFields("bop", DoubleOptions.standard())
                    .build()
                    .array())
            .build();

    @Test
    void fooBarBaz() throws IOException {
        parse(FOO_BAR_BAZ.array(), "[{\"foo\": \"foo\", \"bar\": 42, \"baz\": {\"bip\": 43, \"bop\": 44.0}}]",
                ObjectChunk.chunkWrap(new Object[] { new String[] {"foo"} }),
                ObjectChunk.chunkWrap(new Object[] { new int[] {42} }),
                ObjectChunk.chunkWrap(new Object[] { new long[] {43} }),
                ObjectChunk.chunkWrap(new Object[] { new double[] {44.0} }));
    }

    @Test
    void fooBarBazArray() throws IOException {
        parse(FOO_BAR_BAZ_ARRAY,
                "{\"foo\": \"foo\", \"bar\": 42, \"baz\": [{\"bip\": 43, \"bop\": 44.0}, {\"bip\": 45, \"bop\": 46.0}]}",
                ObjectChunk.chunkWrap(new String[] {"foo"}),
                IntChunk.chunkWrap(new int[] {42}),
                ObjectChunk.chunkWrap(new Object[] {LongChunk.chunkWrap(new long[] {43, 45})}),
                ObjectChunk.chunkWrap(new Object[] {DoubleChunk.chunkWrap(new double[] {44.0, 46.0})}));
    }

    // @Test
    // void standardMissing() throws IOException {
    // parse(IntOptions.standard().array(), "", ObjectChunk.chunkWrap(new Object[] { null }));
    // }
    //
    // @Test
    // void standardNull() throws IOException {
    // parse(IntOptions.standard().array(), "null", ObjectChunk.chunkWrap(new Object[] { null }));
    // }
}
