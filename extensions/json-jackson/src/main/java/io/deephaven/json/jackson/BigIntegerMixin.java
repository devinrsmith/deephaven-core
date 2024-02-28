/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.BigIntegerOptions;
import io.deephaven.json.StringNumberFormat;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class BigIntegerMixin extends Mixin<BigIntegerOptions> {

    public BigIntegerMixin(BigIntegerOptions options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public int outputCount() {
        return 1;
    }

    @Override
    public Stream<List<String>> paths() {
        return Stream.of(List.of());
    }

    @Override
    public Stream<Type<?>> outputTypes() {
        return Stream.of(Type.ofCustom(BigInteger.class));
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new ObjectValueProcessor<>(out.get(0).asWritableObjectChunk(), new Impl());
    }

    private BigInteger parseNumberInt(JsonParser parser) throws IOException {
        if (!options.allowNumberInt()) {
            throw Helpers.mismatch(parser, BigInteger.class);
        }
        return parser.getBigIntegerValue();
    }

    private BigInteger parseNumberFloat(JsonParser parser) throws IOException {
        if (!options.allowNumberFloat()) {
            throw Helpers.mismatch(parser, BigInteger.class);
        }
        return parser.getBigIntegerValue();
    }

    private BigInteger parseString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Helpers.mismatch(parser, BigInteger.class);
        }
        switch (options.stringFormat().orElse(StringNumberFormat.INT)) {
            case INT:
                return Helpers.parseStringAsBigInteger(parser);
            case FLOAT:
                return Helpers.parseStringAsBigDecimal(parser).toBigInteger();
        }
        throw new IllegalStateException();
    }

    private BigInteger parseNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, BigInteger.class);
        }
        return options.onNull().orElse(null);
    }

    private BigInteger parseMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, BigInteger.class);
        }
        return options.onMissing().orElse(null);
    }

    private class Impl implements ObjectValueProcessor.ToObject<BigInteger> {
        @Override
        public BigInteger parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_NUMBER_INT:
                    return parseNumberInt(parser);
                case VALUE_NUMBER_FLOAT:
                    return parseNumberFloat(parser);
                case VALUE_STRING:
                    return parseString(parser);
                case VALUE_NULL:
                    return parseNull(parser);
            }
            throw Helpers.mismatch(parser, BigInteger.class);
        }

        @Override
        public BigInteger parseMissing(JsonParser parser) throws IOException {
            return BigIntegerMixin.this.parseMissing(parser);
        }
    }
}
