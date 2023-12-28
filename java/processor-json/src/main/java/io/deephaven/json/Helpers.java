/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.io.NumberInput;
import com.fasterxml.jackson.core.io.doubleparser.FastDoubleParser;
import com.fasterxml.jackson.core.io.doubleparser.FastFloatParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.util.function.Function;
import java.util.function.ToLongFunction;

final class Helpers {

    static void assertNoCurrentToken(JsonParser parser) {
        if (parser.hasCurrentToken()) {
            throw new IllegalStateException(
                    String.format("Expected no current token. actual=%s", parser.currentToken()));
        }
    }

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

    static class UnexpectedToken extends JsonProcessingException {
        public UnexpectedToken(String msg, JsonLocation loc) {
            super(msg, loc);
        }
    }

    static IOException mismatch(JsonParser parser, Class<?> clazz) {
        final JsonLocation location = parser.currentLocation();
        final String msg = String.format("Unexpected token '%s'", parser.currentToken());
        return new UnexpectedToken(msg, location);
    }

    static IOException mismatchMissing(JsonParser parser, Class<?> clazz) {
        final JsonLocation location = parser.currentLocation();
        return new UnexpectedToken("Unexpected missing token", location);
    }

    static int parseStringAsInt(JsonParser parser) throws IOException {
        // Note: NumberInput#parseInt has different semantics
        final CharSequence cs = textAsCharSequence(parser);
        return Integer.parseInt(cs, 0, cs.length(), 10);
    }

    static long parseStringAsLong(JsonParser parser) throws IOException {
        // Note: NumberInput#parseLong has different semantics
        final CharSequence cs = textAsCharSequence(parser);
        return Long.parseLong(cs, 0, cs.length(), 10);
    }

    static float parseStringAsFloat(JsonParser parser) throws IOException {
        if (parser.isEnabled(StreamReadFeature.USE_FAST_DOUBLE_PARSER)) {
            if (parser.hasTextCharacters()) {
                return FastFloatParser.parseFloat(parser.getTextCharacters(), parser.getTextOffset(),
                        parser.getTextLength());
            } else {
                return FastFloatParser.parseFloat(parser.getText());
            }
        } else {
            return Float.parseFloat(parser.getText());
        }
    }

    static double parseStringAsDouble(JsonParser parser) throws IOException {
        if (parser.isEnabled(StreamReadFeature.USE_FAST_DOUBLE_PARSER)) {
            if (parser.hasTextCharacters()) {
                return FastDoubleParser.parseDouble(parser.getTextCharacters(), parser.getTextOffset(),
                        parser.getTextLength());
            } else {
                return FastDoubleParser.parseDouble(parser.getText());
            }
        } else {
            return Double.parseDouble(parser.getText());
        }
    }

    static BigDecimal parseStringAsBigDecimal(JsonParser parser) throws IOException {
        if (parser.hasTextCharacters()) {
            // If parser supports this, saves us from allocating string
            return NumberInput.parseBigDecimal(parser.getTextCharacters(), parser.getTextOffset(),
                    parser.getTextLength());
        } else {
            return NumberInput.parseBigDecimal(parser.getText());
        }
    }

    static BigInteger parseStringAsBigInteger(JsonParser parser) throws IOException {
        // Todo: PR to jackson to accept textChars version
        return NumberInput.parseBigInteger(parser.getText());
    }
}
