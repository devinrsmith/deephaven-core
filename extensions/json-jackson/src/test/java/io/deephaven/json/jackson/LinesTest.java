//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.IntValue;
import io.deephaven.json.ObjectValue;
import io.deephaven.json.StringValue;
import io.deephaven.json.TestHelper;
import io.deephaven.json.Value;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LinesTest {

    @Test
    void linesProcessor() throws IOException {
        final Value value = ObjectValue.builder()
                .allowUnknownFields(false)
                .putFields("name", StringValue.strict())
                .putFields("age", IntValue.strict())
                .build();

        try (final JsonParser parser = parser("/io/deephaven/json/test-newline-objects.json.txt")) {
            parser.nextToken();

            final LinesProcessor lines = LinesProcessor.of(value, parser, 128);
            {
                assertThat(lines).hasNext();
                final List<WritableChunk<?>> chunks = lines.nextChunks();
                assertThat(chunks.size()).isEqualTo(2);
                TestHelper.check(chunks.get(0), ObjectChunk.chunkWrap(new String[] {"foo", "bar"}));
                TestHelper.check(chunks.get(1), IntChunk.chunkWrap(new int[] {42, 43}));
            }
            assertThat(lines).isExhausted();
        }
    }

    @Test
    void linesProcessorBufferSize() throws IOException {
        final Value value = ObjectValue.builder()
                .allowUnknownFields(false)
                .putFields("name", StringValue.strict())
                .putFields("age", IntValue.strict())
                .build();

        try (final JsonParser parser = parser("/io/deephaven/json/test-newline-objects.json.txt")) {
            parser.nextToken();

            final LinesProcessor lines = LinesProcessor.of(value, parser, 1);
            {
                assertThat(lines).hasNext();
                final List<WritableChunk<?>> chunks = lines.nextChunks();
                assertThat(chunks.size()).isEqualTo(2);
                TestHelper.check(chunks.get(0), ObjectChunk.chunkWrap(new String[] {"foo"}));
                TestHelper.check(chunks.get(1), IntChunk.chunkWrap(new int[] {42}));
            }
            {
                assertThat(lines).hasNext();
                final List<WritableChunk<?>> chunks = lines.nextChunks();
                assertThat(chunks.size()).isEqualTo(2);
                TestHelper.check(chunks.get(0), ObjectChunk.chunkWrap(new String[] {"bar"}));
                TestHelper.check(chunks.get(1), IntChunk.chunkWrap(new int[] {43}));
            }
            assertThat(lines).isExhausted();
        }
    }

    private static URL resource(final String resourceName) {
        return LinesTest.class.getResource(resourceName);
    }

    private static JsonParser parser(final String resourceName) throws IOException {
        return JacksonSource.of(JacksonConfiguration.defaultFactory(), resource(resourceName));
    }
}
