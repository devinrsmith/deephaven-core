/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.CharBuffer;
import java.util.function.Function;
import java.util.function.ToLongFunction;

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

    static CharSequence textAsCharSequence(JsonParser parser) throws IOException {
        return parser.hasTextCharacters()
                ? CharBuffer.wrap(parser.getTextCharacters(), parser.getTextOffset(), parser.getTextLength())
                : parser.getText();
    }

    private static CharSequence textAsCharSequenceSafe(JsonParser parser) {
        try {
            return textAsCharSequence(parser);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static ToLongFunction<? super JsonParser> fromString(ToLongFunction<? super String> f) {
        return (ToLongFunction<JsonParser>) parser -> applyString(f, parser);
    }

    static ToLongFunction<? super JsonParser> fromCharSequence(ToLongFunction<? super CharSequence> f) {
        return (ToLongFunction<JsonParser>) parser -> applyCharSequence(f, parser);
    }

    static <R> Function<? super JsonParser, ? extends R> fromCharSequence(
            Function<? super CharSequence, ? extends R> f) {
        return parser -> applyCharSequence(f, parser);
    }

    static long apply(ToLongFunction<? super JsonParser> f, JsonParser parser) throws IOException {
        try {
            return f.applyAsLong(parser);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    static <R> R apply(Function<? super JsonParser, ? extends R> f, JsonParser parser) throws IOException {
        try {
            return f.apply(parser);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    static long applyString(ToLongFunction<? super String> f, JsonParser parser) {
        final String text;
        try {
            text = parser.getText();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return f.applyAsLong(text);
    }

    static long applyCharSequence(ToLongFunction<? super CharSequence> f, JsonParser parser) {
        return f.applyAsLong(textAsCharSequenceSafe(parser));
    }

    static <R> R applyCharSequence(Function<? super CharSequence, ? extends R> f, JsonParser parser) {
        return f.apply(textAsCharSequenceSafe(parser));
    }

    static JsonToken endToken(JsonToken startToken) {
        switch (startToken) {
            case START_OBJECT:
                return JsonToken.END_OBJECT;
            case START_ARRAY:
                return JsonToken.END_ARRAY;
            case FIELD_NAME:
            case VALUE_STRING:
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
            case VALUE_TRUE:
            case VALUE_FALSE:
            case VALUE_NULL:
                return startToken;
        }
        throw new IllegalStateException("Unexpected startToken: " + startToken);
    }
}
