/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonFactory;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class IntOptionsTest {


    @Test
    void of() {
        final IntOptions intOptions = IntOptions.of();
        try (final WritableIntChunk<Any> c = WritableIntChunk.makeWritableChunk(1)) {
            c.set(0, 42);
            parse(intOptions, "42", c);
        }
    }

    @Test
    void ofMissing() {
        final IntOptions intOptions = IntOptions.of();
        try (final WritableIntChunk<Any> c = WritableIntChunk.makeWritableChunk(1)) {
            c.set(0, QueryConstants.NULL_INT);
            parse(intOptions, "", c);
        }
    }

    @Test
    void ofNull() {
        final IntOptions intOptions = IntOptions.of();
        try (final WritableIntChunk<Any> c = WritableIntChunk.makeWritableChunk(1)) {
            c.set(0, QueryConstants.NULL_INT);
            parse(intOptions, "null", c);
        }
    }

    @Test
    void ofStrict() {
        final IntOptions intOptions = IntOptions.ofStrict();
        try (final WritableIntChunk<Any> c = WritableIntChunk.makeWritableChunk(1)) {
            c.set(0, 42);
            parse(intOptions, "42", c);
        }
    }

    @Test
    void ofStrictMissing() {
        final IntOptions intOptions = IntOptions.ofStrict();
        try (final WritableIntChunk<Any> c = WritableIntChunk.makeWritableChunk(1)) {
            try {
                parse(intOptions, "", c);
                failBecauseExceptionWasNotThrown(IllegalStateException.class);
            } catch (IllegalStateException e) {
                assertThat(e).hasMessageContaining("[<root>]: Unexpected missing value, allowMissing=false");
            }
        }
    }

    @Test
    void ofStrictNull() {
        final IntOptions intOptions = IntOptions.ofStrict();
        try (final WritableIntChunk<Any> c = WritableIntChunk.makeWritableChunk(1)) {
            try {
                parse(intOptions, "null", c);
                failBecauseExceptionWasNotThrown(IllegalStateException.class);
            } catch (IllegalStateException e) {
                assertThat(e).hasMessageContaining("[<root>]: Unexpected null value, allowNull=false");
            }
        }
    }

    @Test
    void ofString() {
        final IntOptions intOptions = IntOptions.of();
        try (final WritableIntChunk<Any> c = WritableIntChunk.makeWritableChunk(1)) {
            try {
                parse(intOptions, "\"42\"", c);
                failBecauseExceptionWasNotThrown(IllegalStateException.class);
            } catch (IllegalStateException e) {
                assertThat(e).hasMessageContaining("Unexpected start token 'VALUE_STRING'");
            }
        }
    }

    @Test
    void ofTrue() {
        final IntOptions intOptions = IntOptions.of();
        try (final WritableIntChunk<Any> c = WritableIntChunk.makeWritableChunk(1)) {
            try {
                parse(intOptions, "true", c);
                failBecauseExceptionWasNotThrown(IllegalStateException.class);
            } catch (IllegalStateException e) {
                assertThat(e).hasMessageContaining("Unexpected start token 'VALUE_TRUE'");
            }
        }
    }

    @Test
    void ofFalse() {
        final IntOptions intOptions = IntOptions.of();
        try (final WritableIntChunk<Any> c = WritableIntChunk.makeWritableChunk(1)) {
            try {
                parse(intOptions, "false", c);
                failBecauseExceptionWasNotThrown(IllegalStateException.class);
            } catch (IllegalStateException e) {
                assertThat(e).hasMessageContaining("Unexpected start token 'VALUE_FALSE'");
            }
        }
    }

    @Test
    void ofFloat() {
        final IntOptions intOptions = IntOptions.of();
        try (final WritableIntChunk<Any> c = WritableIntChunk.makeWritableChunk(1)) {
            try {
                parse(intOptions, "42.0", c);
                failBecauseExceptionWasNotThrown(IllegalStateException.class);
            } catch (IllegalStateException e) {
                assertThat(e).hasMessageContaining("Unexpected start token 'VALUE_NUMBER_FLOAT'");
            }
        }
    }

    @Test
    void ofObject() {
        final IntOptions intOptions = IntOptions.of();
        try (final WritableIntChunk<Any> c = WritableIntChunk.makeWritableChunk(1)) {
            try {
                parse(intOptions, "{}", c);
                failBecauseExceptionWasNotThrown(IllegalStateException.class);
            } catch (IllegalStateException e) {
                assertThat(e).hasMessageContaining("Unexpected start token 'START_OBJECT'");
            }
        }
    }

    @Test
    void ofArray() {
        final IntOptions intOptions = IntOptions.of();
        try (final WritableIntChunk<Any> c = WritableIntChunk.makeWritableChunk(1)) {
            try {
                parse(intOptions, "[]", c);
                failBecauseExceptionWasNotThrown(IllegalStateException.class);
            } catch (IllegalStateException e) {
                assertThat(e).hasMessageContaining("Unexpected start token 'START_ARRAY'");
            }
        }
    }

    static void parse(ValueOptions options, String json, Chunk<?>... expected) {
        final ObjectProcessorJsonValue processor = new ObjectProcessorJsonValue(new JsonFactory(), options);
        final List<WritableChunk<?>> out = processor
                .outputTypes()
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(x -> x.makeWritableChunk(1))
                .collect(Collectors.toList());
        try {
            assertThat(out.size()).isEqualTo(expected.length);
            assertThat(out.stream().map(Chunk::getChunkType).collect(Collectors.toList()))
                    .isEqualTo(Stream.of(expected).map(Chunk::getChunkType).collect(Collectors.toList()));
            for (WritableChunk<?> wc : out) {
                wc.setSize(0);
            }
            try (final WritableObjectChunk<byte[], Any> in = WritableObjectChunk.makeWritableChunk(1)) {
                in.set(0, json.getBytes(StandardCharsets.UTF_8));
                processor.processAll(in, out);
            }
            for (int i = 0; i < expected.length; ++i) {
                check(out.get(i), expected[i]);
            }
        } finally {
            for (WritableChunk<?> wc : out) {
                wc.close();
            }
        }
    }

    static void check(Chunk<?> actual, Chunk<?> expected) {
        assertThat(actual.getChunkType()).isEqualTo(expected.getChunkType());
        assertThat(actual.size()).isEqualTo(expected.size());
        switch (actual.getChunkType()) {
            case Boolean:
                break;
            case Char:
                break;
            case Byte:
                break;
            case Short:
                break;
            case Int:
                check(actual.asIntChunk(), expected.asIntChunk());
                break;
            case Long:
                break;
            case Float:
                break;
            case Double:
                break;
            case Object:
                break;
        }
    }

    private static void check(IntChunk<?> actual, IntChunk<?> expected) {
        final int size = actual.size();
        for (int i = 0; i < size; ++i) {
            assertThat(actual.get(i)).isEqualTo(expected.get(i));
        }
    }
}
