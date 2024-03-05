/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.deephaven.json.TestHelper.parse;

public class ObjectArrayOptionsTest {

    @Test
    void nested() throws IOException {
        parse(ObjectOptions.builder()
                .putFields("strings", StringOptions.standard())
                .putFields("ints", IntOptions.standard())
                .putFields("longs", LongOptions.standard())
                .build()
                .array(),
                "[{\"strings\": \"foo\", \"ints\": 1, \"longs\": 2}, {}, {\"strings\": \"bar\", \"ints\": 3, \"longs\": 4}]",
                ObjectChunk.chunkWrap(new Object[] {new String[] {"foo", null, "bar"}}),
                ObjectChunk.chunkWrap(new Object[] {new int[] {1, QueryConstants.NULL_INT, 3}}),
                ObjectChunk.chunkWrap(new Object[] {new long[] {2, QueryConstants.NULL_LONG, 4}}));
    }
}
