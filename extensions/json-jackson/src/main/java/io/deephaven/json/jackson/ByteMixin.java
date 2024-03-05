/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ByteOptions;
import io.deephaven.json.jackson.ByteValueProcessor.ToByte;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class ByteMixin extends Mixin<ByteOptions> implements ToByte {
    public ByteMixin(ByteOptions options, JsonFactory factory) {
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
        return Stream.of(Type.byteType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new ByteValueProcessor(out.get(0).asWritableByteChunk(), this);
    }

    @Override
    ArrayProcessor arrayProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        // array of arrays
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public byte parseValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_NUMBER_INT:
                return parseFromInt(parser);
            case VALUE_NUMBER_FLOAT:
                return parseFromDecimal(parser);
            case VALUE_STRING:
                return parseFromString(parser);
            case VALUE_NULL:
                return parseFromNull(parser);
        }
        throw Helpers.mismatch(parser, int.class);
    }

    @Override
    public byte parseMissing(JsonParser parser) throws IOException {
        return ByteMixin.this.parseFromMissing(parser);
    }

    private byte parseFromInt(JsonParser parser) throws IOException {
        if (!options.allowNumberInt()) {
            throw Helpers.mismatch(parser, byte.class);
        }
        return Helpers.parseIntAsByte(parser);
    }

    private byte parseFromDecimal(JsonParser parser) throws IOException {
        if (!options.allowDecimal()) {
            throw Helpers.mismatch(parser, byte.class);
        }
        return Helpers.parseDecimalAsTruncatedByte(parser);
    }

    private byte parseFromString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Helpers.mismatch(parser, byte.class);
        }
        return options.allowDecimal()
                ? Helpers.parseDecimalStringAsTruncatedByte(parser)
                : Helpers.parseStringAsByte(parser);
    }

    private byte parseFromNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, byte.class);
        }
        return options.onNull().orElse(QueryConstants.NULL_BYTE);
    }

    private byte parseFromMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, byte.class);
        }
        return options.onMissing().orElse(QueryConstants.NULL_BYTE);
    }
}
