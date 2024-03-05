/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.BoolOptions;
import io.deephaven.qst.type.Type;
import io.deephaven.util.BooleanUtils;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class BoolMixin extends Mixin<BoolOptions> {
    public BoolMixin(BoolOptions options, JsonFactory factory) {
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
        return Stream.of(Type.booleanType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        // bool to byte
        return new ByteValueProcessor(out.get(0).asWritableByteChunk(), boolImpl());
    }

    @Override
    ArrayProcessor arrayProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        // array of arrays
        throw new UnsupportedOperationException("todo");
    }

    ByteValueProcessor.ToByte boolImpl() {
        return new Impl();
    }

    private byte parseFromString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Helpers.mismatch(parser, boolean.class);
        }
        return Helpers.parseStringAsByteBool(parser);
    }

    private byte parseFromNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, boolean.class);
        }
        return BooleanUtils.booleanAsByte(options.onNull().orElse(null));
    }

    private byte parseFromMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, boolean.class);
        }
        return BooleanUtils.booleanAsByte(options.onMissing().orElse(null));
    }

    private class Impl implements ByteValueProcessor.ToByte {
        @Override
        public byte parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_TRUE:
                    return BooleanUtils.TRUE_BOOLEAN_AS_BYTE;
                case VALUE_FALSE:
                    return BooleanUtils.FALSE_BOOLEAN_AS_BYTE;
                case VALUE_NULL:
                    return parseFromNull(parser);
                case VALUE_STRING:
                    return parseFromString(parser);
            }
            throw Helpers.mismatch(parser, boolean.class);
        }

        @Override
        public byte parseMissing(JsonParser parser) throws IOException {
            return BoolMixin.this.parseFromMissing(parser);
        }
    }
}
