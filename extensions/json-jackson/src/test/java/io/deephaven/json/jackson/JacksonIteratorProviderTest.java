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

import static org.assertj.core.api.Assertions.assertThat;

class JacksonIteratorProviderTest {

    private static ObjectValue options() {
        return ObjectValue.builder()
                .allowUnknownFields(false)
                .putFields("name", StringValue.strict())
                .putFields("age", IntValue.strict())
                .build();
    }

    @Test
    void iterator() throws IOException {
        checkIterator(JacksonIteratorSpec.stream(options()), "/io/deephaven/json/test-newline-objects.json.txt");
        checkIterator(JacksonIteratorSpec.array(options()), "/io/deephaven/json/test-array-objects.json");
    }

    @Test
    void iteratorSmallBufferSize() throws IOException {
        checkIterator2(JacksonIteratorSpec.stream(options()), "/io/deephaven/json/test-newline-objects.json.txt");
        checkIterator2(JacksonIteratorSpec.array(options()), "/io/deephaven/json/test-array-objects.json");
    }

    private static void checkIterator(final JacksonIteratorSpec jip, final String resourceName) throws IOException {
        try (final JsonParser parser = parser(resourceName)) {
            parser.nextToken();
            Helper.check(jip.iterator(parser, 128));
        }
    }

    private static void checkIterator2(final JacksonIteratorSpec jip, final String resourceName)
            throws IOException {
        try (final JsonParser parser = parser(resourceName)) {
            parser.nextToken();
            Helper.check2(jip.iterator(parser, 1));
        }
    }

    private static URL resource(final String resourceName) {
        return JacksonIteratorProviderTest.class.getResource(resourceName);
    }

    private static JsonParser parser(final String resourceName) throws IOException {
        return JacksonSource.of(JacksonConfiguration.defaultFactory(), resource(resourceName));
    }
}
