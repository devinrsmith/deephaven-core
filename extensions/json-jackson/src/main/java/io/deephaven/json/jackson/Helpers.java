/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import ch.randelshofer.fastdoubleparser.JavaBigDecimalParser;
import ch.randelshofer.fastdoubleparser.JavaBigIntegerParser;
import ch.randelshofer.fastdoubleparser.JavaDoubleParser;
import ch.randelshofer.fastdoubleparser.JavaFloatParser;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadFeature;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;

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
        if (!parser.hasToken(expected)) {
            throw new IllegalStateException(
                    String.format("Unexpected current token. expected=%s, actual=%s", expected, parser.currentToken()));
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

    static int parseNumberIntAsInt(JsonParser parser) throws IOException {
        return parser.getIntValue();
    }

    static int parseNumberFloatAsInt(JsonParser parser) throws IOException {
        // May lose info
        return parser.getIntValue();
    }

    static long parseNumberIntAsLong(JsonParser parser) throws IOException {
        return parser.getLongValue();
    }

    static long parseNumberFloatAsLong(JsonParser parser) throws IOException {
        // May lose info
        return parser.getLongValue();
    }

    static BigDecimal parseNumberFloatAsBigDecimal(JsonParser parser) throws IOException {
        return parser.getDecimalValue();
    }

    static float parseNumberAsFloat(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return parser.getFloatValue();
    }

    static double parseNumberAsDouble(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return parser.getDoubleValue();
    }

    static int parseStringAsInt(JsonParser parser) throws IOException {
        // 23mm / s; 19 bytes garbage
        // return parser.getValueAsInt();
        // No apparent difference in this case like in the long case.
        // 23mm / s; 19 bytes garbage
        if (parser.hasTextCharacters()) {
            // TODO: potential to write parseInt optimized for char[]
            final int len = parser.getTextLength();
            final CharSequence cs = CharBuffer.wrap(parser.getTextCharacters(), parser.getTextOffset(), len);
            return Integer.parseInt(cs, 0, len, 10);
        } else {
            return Integer.parseInt(parser.getText());
        }
    }

    static long parseStringAsLong(JsonParser parser) throws IOException {
        // 11mm / s; 88 bytes garbage
        // return parser.getValueAsLong();
        // 17mm / s; 24 bytes garbage
        if (parser.hasTextCharacters()) {
            // TODO: potential to write parseInt optimized for char[]
            final int len = parser.getTextLength();
            final CharSequence cs = CharBuffer.wrap(parser.getTextCharacters(), parser.getTextOffset(), len);
            return Long.parseLong(cs, 0, len, 10);
        } else {
            return Long.parseLong(parser.getText());
        }
    }

    static long parseStringConvertToLong(JsonParser parser) throws IOException {
        // To ensure 64-bit in cases where the string is a float, we need BigDecimal
        return parseStringAsBigDecimal(parser).longValue();
    }

    static float parseStringAsFloat(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return parser.isEnabled(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
                ? parseStringAsFloatFast(parser)
                : Float.parseFloat(parser.getText());
    }

    static double parseStringAsDouble(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        // 14mm / s; 73 bytes of garbage
        // return p.getValueAsDouble();
        // 20mm / s; 8 bytes of garbage
        return parser.isEnabled(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
                ? parseStringAsDoubleFast(parser)
                : Double.parseDouble(parser.getText());
    }

    static BigInteger parseStringAsBigInteger(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return parser.isEnabled(StreamReadFeature.USE_FAST_BIG_NUMBER_PARSER)
                ? parseStringAsBigIntegerFast(parser)
                : new BigInteger(parser.getText());
    }

    static BigDecimal parseStringAsBigDecimal(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return parser.isEnabled(StreamReadFeature.USE_FAST_BIG_NUMBER_PARSER)
                ? parseStringAsBigDecimalFast(parser)
                : new BigDecimal(parser.getText());
    }

    private static float parseStringAsFloatFast(JsonParser p) throws IOException {
        return p.hasTextCharacters()
                ? JavaFloatParser.parseFloat(p.getTextCharacters(), p.getTextOffset(), p.getTextLength())
                : JavaFloatParser.parseFloat(p.getText());
    }

    private static double parseStringAsDoubleFast(JsonParser p) throws IOException {
        return p.hasTextCharacters()
                ? JavaDoubleParser.parseDouble(p.getTextCharacters(), p.getTextOffset(), p.getTextLength())
                : JavaDoubleParser.parseDouble(p.getText());
    }

    private static BigInteger parseStringAsBigIntegerFast(JsonParser p) throws IOException {
        return p.hasTextCharacters()
                ? JavaBigIntegerParser.parseBigInteger(p.getTextCharacters(), p.getTextOffset(), p.getTextLength())
                : JavaBigIntegerParser.parseBigInteger(p.getText());
    }

    private static BigDecimal parseStringAsBigDecimalFast(JsonParser p) throws IOException {
        return p.hasTextCharacters()
                ? JavaBigDecimalParser.parseBigDecimal(p.getTextCharacters(), p.getTextOffset(), p.getTextLength())
                : JavaBigDecimalParser.parseBigDecimal(p.getText());
    }

    private static int parseInt(char[] str, int offset, int length) {
        // leading +?
        final boolean negate = str[offset] == '-';
        int res = 0;
        for (int i = negate ? offset + 1 : offset; i < offset + length; ++i) {
            final char ch = str[i];
            res = res * 10 - (ch - '0');
        }
        // don't worry about edge case atm.
        return negate ? -res : res;
    }

    private static long parseLong(char[] str, int offset, int length) {
        // leading +?
        final boolean negate = str[offset] == '-';
        long res = 0;
        for (int i = negate ? offset + 1 : offset; i < offset + length; ++i) {
            final char ch = str[i];
            res = res * 10 - (ch - '0');
        }
        // don't worry about edge case atm.
        return negate ? -res : res;
    }

    public static boolean isDigit(char ch) {
        return ch >= '0' && ch <= '9';
    }
}
