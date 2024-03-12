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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ObjectOptionsTest {

    public static final ObjectOptions OBJECT_AGE_FIELD = ObjectOptions.builder()
            .putFields("age", IntOptions.standard())
            .build();

    private static final ObjectOptions OBJECT_NAME_AGE_FIELD = ObjectOptions.builder()
            .putFields("name", StringOptions.standard())
            .putFields("age", IntOptions.standard())
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

    @Test
    void caseSensitive() throws IOException {
        final ObjectOptions options = ObjectOptions.builder()
                .addFields(ObjectFieldOptions.builder()
                        .name("Foo")
                        .options(IntOptions.standard())
                        .caseInsensitiveMatch(true)
                        .build())
                .build();
        parse(options, List.of("{\"Foo\": 42}", "{\"fOO\": 43}"),
                IntChunk.chunkWrap(new int[] {42, 43}));
    }

    @Test
    void caseInsensitive() throws IOException {
        final ObjectFieldOptions f1 = ObjectFieldOptions.of("Foo", IntOptions.standard());
        final ObjectFieldOptions f2 = ObjectFieldOptions.of("foo", IntOptions.standard());
        final ObjectOptions options = ObjectOptions.builder()
                .addFields(f1)
                .addFields(f2)
                .build();
        parse(options, List.of("{\"Foo\": 42, \"foo\": 43}"),
                IntChunk.chunkWrap(new int[] {42}),
                IntChunk.chunkWrap(new int[] {43}));
    }

    @Test
    void alias() throws IOException {
        final ObjectOptions options = ObjectOptions.builder()
                .addFields(ObjectFieldOptions.builder()
                        .name("FooBar")
                        .options(IntOptions.standard())
                        .addAliases("Foo_Bar")
                        .build())
                .build();
        parse(options, List.of("{\"FooBar\": 42}", "{\"Foo_Bar\": 43}"),
                IntChunk.chunkWrap(new int[] {42, 43}));
    }

    @Test
    void caseInsensitiveAlias() throws IOException {
        final ObjectOptions options = ObjectOptions.builder()
                .addFields(ObjectFieldOptions.builder()
                        .name("FooBar")
                        .options(IntOptions.standard())
                        .addAliases("Foo_Bar")
                        .caseInsensitiveMatch(true)
                        .build())
                .build();
        parse(options, List.of("{\"fooBar\": 42}", "{\"fOO_BAR\": 43}"),
                IntChunk.chunkWrap(new int[] {42, 43}));
    }

    @Test
    void caseSensitiveFields() {
        final ObjectFieldOptions f1 = ObjectFieldOptions.of("Foo", IntOptions.standard());
        final ObjectFieldOptions f2 = ObjectFieldOptions.of("foo", IntOptions.standard());
        final ObjectOptions options = ObjectOptions.builder()
                .addFields(f1)
                .addFields(f2)
                .build();
        assertThat(options.fields()).containsExactly(f1, f2);
    }

    @Test
    void caseInsensitiveOverlap() {
        final ObjectFieldOptions f1 = ObjectFieldOptions.of("Foo", IntOptions.standard());
        final ObjectFieldOptions f2 = ObjectFieldOptions.builder()
                .name("foo")
                .options(IntOptions.standard())
                .caseInsensitiveMatch(true)
                .build();
        try {
            ObjectOptions.builder()
                    .addFields(f1)
                    .addFields(f2)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("Found overlapping field name 'foo'");
        }
    }
}
