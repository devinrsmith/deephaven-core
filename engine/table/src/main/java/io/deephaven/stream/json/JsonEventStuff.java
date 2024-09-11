//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stream.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

public interface JsonEventStuff {

    void writeToSink(JsonParser parser) throws IOException;

    static void startObject(JsonParser parser) {
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            throw new IllegalStateException();
        }
    }

    static void endObject(JsonParser parser) {
        if (parser.currentToken() != JsonToken.END_OBJECT) {
            throw new IllegalStateException();
        }
    }

    static void startArray(JsonParser parser) {
        if (parser.currentToken() != JsonToken.START_ARRAY) {
            throw new IllegalStateException();
        }
    }

    static void endArray(JsonParser parser) {
        if (parser.currentToken() != JsonToken.END_ARRAY) {
            throw new IllegalStateException();
        }
    }

    static void fieldName(JsonParser parser, String fieldName) throws IOException {
        if (parser.currentToken() != JsonToken.FIELD_NAME) {
            throw new IllegalStateException();
        }
        if (!fieldName.equals(parser.currentName())) {
            throw new IllegalStateException();
        }
    }

    static String stringValue(JsonParser parser) throws IOException {
        if (parser.currentToken() != JsonToken.VALUE_STRING) {
            throw new IllegalStateException();
        }
        return parser.getText();
    }

    static int intValue(JsonParser parser) throws IOException {
        if (parser.currentToken() != JsonToken.VALUE_NUMBER_INT) {
            throw new IllegalStateException();
        }
        return parser.getIntValue();
    }

    static long longValue(JsonParser parser) throws IOException {
        if (parser.currentToken() != JsonToken.VALUE_NUMBER_INT) {
            throw new IllegalStateException();
        }
        return parser.getLongValue();
    }
}
