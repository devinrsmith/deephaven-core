//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class JacksonMetaProcessor implements ValueProcessor {

    private WritableObjectChunk<JsonToken, ?> token;
    private WritableObjectChunk<String, ?> stringValue;
    private WritableIntChunk<?> intValue;
    private WritableLongChunk<?> longValue;
    private WritableObjectChunk<BigInteger, ?> bigIntegerValue;

    public JacksonIterator iterator(final JsonParser parser, final int chunkCapacity) {
        return new MetaIterator(parser, chunkCapacity);
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        final JsonToken token = parser.currentToken();
        this.token.add(token);
        boolean isString = false;
        boolean isInt = false;
        boolean isLong = false;
        boolean isBigInt = false;
        switch (token) {
            case FIELD_NAME:
                stringValue.add(parser.currentName());
                isString = true;
                break;
            case VALUE_STRING:
                stringValue.add(parser.getText());
                isString = true;
                break;
            case VALUE_NUMBER_INT:
                switch (parser.getNumberType()) {
                    case INT:
                        intValue.add(parser.getIntValue());
                        isInt = true;
                        break;
                    case LONG:
                        longValue.add(parser.getLongValue());
                        isLong = true;
                        break;
                    case BIG_INTEGER:
                        bigIntegerValue.add(parser.getBigIntegerValue());
                        isBigInt = true;
                        break;
                }
                break;
            case VALUE_NUMBER_FLOAT:
                // parser.getNumberTypeFP()
                // see com.fasterxml.jackson.core.JsonParser.NumberTypeFP.UNKNOWN javadoc
                stringValue.add(parser.getText());
                isString = true;
                break;
        }
        if (!isString) {
            stringValue.add(null);
        }
        if (!isInt) {
            intValue.add(QueryConstants.NULL_INT);
        }
        if (!isLong) {
            longValue.add(QueryConstants.NULL_LONG);
        }
        if (!isBigInt) {
            bigIntegerValue.add(null);
        }
    }

    @Override
    public void processMissing(JsonParser parser) {
        token.add(null);
        stringValue.add(null);
        intValue.add(QueryConstants.NULL_INT);
        longValue.add(QueryConstants.NULL_LONG);
        bigIntegerValue.add(null);
    }

    @Override
    public void setContext(List<WritableChunk<?>> out) {
        token = out.get(0).asWritableObjectChunk();
        stringValue = out.get(1).asWritableObjectChunk();
        intValue = out.get(2).asWritableIntChunk();
        longValue = out.get(3).asWritableLongChunk();
        bigIntegerValue = out.get(4).asWritableObjectChunk();
    }

    @Override
    public void clearContext() {
        token = null;
        stringValue = null;
        intValue = null;
        longValue = null;
        bigIntegerValue = null;
    }

    @Override
    public int numColumns() {
        return numTypes();
    }

    @Override
    public Stream<Type<?>> columnTypes() {
        return types();
    }

    private class MetaIterator extends JacksonIterator {
        MetaIterator(JsonParser parser, int chunkCapacity) {
            super(JacksonMetaProcessor.this, parser, chunkCapacity);
        }

        @Override
        public boolean hasNext() {
            return parser.hasCurrentToken();
        }
    }

    public enum Spec implements JacksonValue2 {
        META_SPEC;

        @Override
        public JacksonIterator iterator(JsonParser parser, int chunkCapacity) {
            return new JacksonMetaProcessor().iterator(parser, chunkCapacity);
        }

        @Override
        public List<Type<?>> outputTypes() {
            return types().collect(Collectors.toList());
        }

        @Override
        public int outputSize() {
            return numTypes();
        }

        @Override
        public List<String> names() {
            return suggestedNames();
        }

        @Override
        public List<String> names(Function<List<String>, String> f) {
            return suggestedNames()
                    .stream()
                    .map(List::of)
                    .map(f)
                    .collect(Collectors.toList());
        }
    }

    public static int numTypes() {
        return 5;
    }

    public static Stream<Type<?>> types() {
        return Stream.of(
                Type.ofCustom(JsonToken.class),
                Type.stringType(),
                Type.intType(),
                Type.longType(),
                Type.ofCustom(BigInteger.class));
    }

    public static List<String> suggestedNames() {
        return List.of("Token", "StringValue", "IntValue", "LongValue", "BigIntegerValue");
    }
}
