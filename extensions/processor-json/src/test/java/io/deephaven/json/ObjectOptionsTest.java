/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;

public class ObjectOptionsTest {

    public static final ObjectOptions OBJECT_AGE_FIELD = ObjectOptions.builder()
            .putFields("age", IntOptions.standard())
            .build();

    private static final ObjectOptions OBJECT_NAME_AGE_FIELD = ObjectOptions.builder()
            .putFields("name", StringOptions.standard())
            .putFields("age", IntOptions.standard())
            .putFields("properties", ObjectOptions.builder()
                    .putFields("foo", null)
                    .putFields("bar", null)
                    .build())
            .build();

    @Test
    void ofAge() throws IOException {
        parse(OBJECT_AGE_FIELD, List.of(
                "",
                "null",
                "{}",
                "{\"age\": 42}",
                "{\"name\": \"Devin\", \"age\": 43}"),
                IntChunk.chunkWrap(
                        new int[] {QueryConstants.NULL_INT, QueryConstants.NULL_INT, QueryConstants.NULL_INT, 42, 43}));
    }

    @Test
    void ofNameAge() throws IOException {
        parse(OBJECT_NAME_AGE_FIELD, List.of(
                // "",
                // "null",
                // "{}",
                // "{\"age\": 42}",
                "{\"name\": \"Devin\", \"age\": 43}"),
                ObjectChunk.chunkWrap(new String[] {"Devin"}),
                IntChunk.chunkWrap(
                        new int[] {43}));
    }
}
