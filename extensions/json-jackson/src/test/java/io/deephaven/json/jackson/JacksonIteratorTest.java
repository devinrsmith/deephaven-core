//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.IntValue;
import io.deephaven.json.ObjectValue;
import io.deephaven.json.StringValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;

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
