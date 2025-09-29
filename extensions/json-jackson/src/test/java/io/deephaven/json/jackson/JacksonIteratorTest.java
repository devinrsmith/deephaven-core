//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.InstantValue;
import io.deephaven.json.IntValue;
import io.deephaven.json.LongValue;
import io.deephaven.json.ObjectField;
import io.deephaven.json.ObjectValue;
import io.deephaven.json.StringValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static org.assertj.core.api.Assertions.assertThat;

class JacksonIteratorTest {

    private static ObjectValue options() {
        return ObjectValue.builder()
                .allowUnknownFields(false)
                .putFields("name", StringValue.strict())
                .putFields("age", IntValue.strict())
                .build();
    }

    private static JacksonValue2 arraySpec() {
        return JacksonValue2.array(options());
    }

    private static JacksonValue2 streamSpec() {
        return JacksonValue2.stream(options());
    }

    private static JacksonValue2 objectEntriesSpec() {
        return JacksonValue2.objectEntries(StringValue.strict(), IntValue.strict());
    }

    @Test
    void arrayObjects() throws IOException {
        checkIterator(arraySpec(), "/io/deephaven/json/test-array-objects.json");
    }

    @Test
    void newlineDelimitedJson() throws IOException {
        checkIterator(streamSpec(), "/io/deephaven/json/test-newline-objects.json.txt");
    }

    @Test
    void compactStreamJson() throws IOException {
        checkIterator(streamSpec(), "/io/deephaven/json/test-compact-objects.json.txt");
    }

    @Test
    void nestedArrayJson() throws IOException {
        try (final JsonParser parser = parser("/io/deephaven/json/test-nested-array-objects.json")) {
            assertThat(parser.nextToken()).isEqualTo(START_OBJECT);
            assertThat(parser.nextToken()).isEqualTo(FIELD_NAME);
            assertThat(parser.currentName()).isEqualTo("data");
            assertThat(parser.nextToken()).isEqualTo(START_ARRAY);

            // We don't have any easy way to declare this right now, but you can start an iterator from a nested context
            // as long as the parser is at the correct place
            Helper.checkRows(arraySpec().iterator(parser, 128));

            assertThat(parser.currentToken()).isEqualTo(END_ARRAY);
            assertThat(parser.nextToken()).isEqualTo(END_OBJECT);
            assertThat(parser.nextToken()).isNull();
        }
    }

    @Test
    void objectEntriesJson() throws IOException {
        checkIterator(objectEntriesSpec(), "/io/deephaven/json/test-object-entries.json");
    }

    @Test
    void name2() throws IOException {

        final ObjectValue ov = ObjectValue.builder()
                .putFields("id", LongValue.lenient())
                .putFields("type", StringValue.strict())
                .putFields("actor", ObjectValue.strict(Map.of("id", LongValue.strict())))
                .putFields("repo", ObjectValue.strict(Map.of("id", LongValue.strict())))
//                .putFields("payload", ObjectValue.strict(Map.of("action", StringValue.strict())))
                .putFields("created_at", InstantValue.strict())
                .putFields("org", ObjectValue.builder()
                        .putFields("id", LongValue.standard())
                        .putFields("login", StringValue.standard())
                        .build())
                .build();
        JacksonValue2 stream = JacksonValue2.stream(ov);

        try (
                final InputStream in = new GZIPInputStream(new URL("https://data.gharchive.org/2025-01-01-15.json.gz").openStream(), 4096);
                final JsonParser parser = JacksonSource.of(JacksonConfiguration.defaultFactory(), in)) {
            parser.nextToken();
            final JacksonIterator iterator = stream.iterator(parser, 128);
            while (iterator.hasNext()) {
                final List<WritableChunk<?>> chunks = iterator.nextChunks();
                LongChunk<?> ids = chunks.get(0).asLongChunk();
                ObjectChunk<String, ?> types = chunks.get(1).asObjectChunk();
                LongChunk<?> actorIds = chunks.get(2).asLongChunk();
                LongChunk<?> repoIds = chunks.get(3).asLongChunk();
                LongChunk<?> createdAts = chunks.get(4).asLongChunk();
                LongChunk<?> orgIds = chunks.get(5).asLongChunk();
                ObjectChunk<String, ?> orgLogins = chunks.get(6).asObjectChunk();


                int size = ids.size();
                for (int i = 0; i < size; ++i) {

                    System.out.println(types.get(i));

                    if (orgIds.get(i) == Long.MIN_VALUE && orgLogins.get(i) != null) {
                        System.out.println(orgLogins.get(i));
                    }
                }

                for (WritableChunk<?> chunk : chunks) {
                    chunk.close();
                }
            }
        }

    }

    @Test
    void name() throws IOException {
        final ObjectValue ov = ObjectValue.builder()
                .putFields("id", LongValue.lenient())
                .putFields("type", StringValue.strict())
                .putFields("actor", ObjectValue.strict(Map.of("id", LongValue.strict())))
                .putFields("repo", ObjectValue.strict(Map.of("id", LongValue.strict())))
//                .putFields("payload", ObjectValue.strict(Map.of("action", StringValue.strict())))
                .putFields("created_at", InstantValue.strict())
                .putFields("org", ObjectValue.builder()
                        .putFields("id", LongValue.standard())
                        .putFields("login", StringValue.standard())
                        .build())
                .build();
        JacksonValue2 stream = JacksonValue2.stream(ov);


        try (final JsonParser parser = parser("/io/deephaven/json/2015-01-01-15.json")) {
            parser.nextToken();
            final JacksonIterator iterator = stream.iterator(parser, 128);
            while (iterator.hasNext()) {
                final List<WritableChunk<?>> chunks = iterator.nextChunks();
                LongChunk<?> ids = chunks.get(0).asLongChunk();
                ObjectChunk<String, ?> types = chunks.get(1).asObjectChunk();
                LongChunk<?> actorIds = chunks.get(2).asLongChunk();
                LongChunk<?> repoIds = chunks.get(3).asLongChunk();
                LongChunk<?> createdAts = chunks.get(4).asLongChunk();
                LongChunk<?> orgIds = chunks.get(5).asLongChunk();
                ObjectChunk<String, ?> orgLogins = chunks.get(6).asObjectChunk();


                int size = ids.size();
                for (int i = 0; i < size; ++i) {

                    System.out.println(types.get(i));

                    if (orgIds.get(i) == Long.MIN_VALUE && orgLogins.get(i) != null) {
                        System.out.println(orgLogins.get(i));
                    }
                }

                for (WritableChunk<?> chunk : chunks) {
                    chunk.close();
                }
            }
        }
    }

    private static void checkIterator(final JacksonValue2 jip, final String resourceName) throws IOException {
        // Check we can consume it in one shot
        try (final JsonParser parser = parser(resourceName)) {
            parser.nextToken();
            Helper.checkRows(jip.iterator(parser, 128));
        }

        // Check we can consume it in two shots if the buffer size is smaller than the total number of rows
        try (final JsonParser parser = parser(resourceName)) {
            parser.nextToken();
            Helper.checkRows2Shot(jip.iterator(parser, 1));
        }

        // Check that we _can_ start up a new iterator mid-stream
        // (for example, if we wanted to change the buffer size, we could)
        try (final JsonParser parser = parser(resourceName)) {
            parser.nextToken();
            {
                final JacksonIterator it = jip.iterator(parser, 1);
                Helper.checkExactlyRow1(it);
                assertThat(it).hasNext();
            }
            {
                final JacksonIterator it = jip.iterator(parser, 1);
                Helper.checkExactlyRow2(it);
                assertThat(it).isExhausted();
            }
        }

    }

    private static URL resource(final String resourceName) {
        return JacksonIteratorTest.class.getResource(resourceName);
    }

    private static JsonParser parser(final String resourceName) throws IOException {
        return JacksonSource.of(JacksonConfiguration.defaultFactory(), resource(resourceName));
    }
}
