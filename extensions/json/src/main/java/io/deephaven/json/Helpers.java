/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

final class Helpers {
    static void assertNextToken(JsonParser parser, JsonToken expected) throws IOException {
        final JsonToken actual = parser.nextToken();
        if (actual != expected) {
            throw new IllegalStateException(
                    String.format("Unexpected next token. expected=%s, actual=%s", expected, actual));
        }
    }

    static void assertCurrentToken(JsonParser parser, JsonToken expected) {
        final JsonToken actual = parser.currentToken();
        if (actual != expected) {
            throw new IllegalStateException(
                    String.format("Unexpected current token. expected=%s, actual=%s", expected, actual));
        }
    }

    static void assertNextTokenIsValue(JsonParser parser) throws IOException {
        final JsonToken actual = parser.nextToken();
        if (!actual.isScalarValue() && !actual.isStructStart()) {
            throw new IllegalStateException(
                    String.format("Unexpected next token. expected value type, actual=%s", actual));
        }
    }
}
