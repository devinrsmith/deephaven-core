/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.BoolOptions;
import io.deephaven.json.jackson.ByteValueProcessor.ToByte;
import io.deephaven.json.jackson.ObjectValueProcessor.ToObject;
import io.deephaven.qst.type.Type;
import io.deephaven.util.BooleanUtils;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class BoolMixin extends Mixin<BoolOptions> implements ToByte {
    public BoolMixin(BoolOptions options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public int numColumns() {
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
        return new ByteValueProcessor(out.get(0).asWritableByteChunk(), this);
    }

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
        return parseFromMissing(parser);
    }

    @Override
    ArrayProcessor arrayProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        return new ArrayProcessorGenericImpl<>(out.get(0).asWritableObjectChunk()::add, allowMissing, allowNull, null,
                null, new ToBoolean(), Boolean.class, Boolean[].class);
    }

    final class ToBoolean implements ToObject<Boolean> {
        @Override
        public Boolean parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_TRUE:
                    return Boolean.TRUE;
                case VALUE_FALSE:
                    return Boolean.FALSE;
                case VALUE_NULL:
                    return parseFromNullBoolean(parser);
                case VALUE_STRING:
                    return parseFromStringBoolean(parser);
            }
            throw Helpers.mismatch(parser, boolean.class);
        }

        @Override
        public Boolean parseMissing(JsonParser parser) throws IOException {
            return parseFromMissingBoolean(parser);
        }
    }

    private byte parseFromString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Helpers.mismatch(parser, boolean.class);
        }
        if (!options.allowNull()) {
            final byte res = Helpers.parseStringAsByteBool(parser, BooleanUtils.NULL_BOOLEAN_AS_BYTE);
            if (res == BooleanUtils.NULL_BOOLEAN_AS_BYTE) {
                throw Helpers.mismatch(parser, boolean.class);
            }
            return res;
        }
        return Helpers.parseStringAsByteBool(parser, BooleanUtils.booleanAsByte(options.onNull().orElse(null)));
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

    private Boolean parseFromNullBoolean(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, Boolean.class);
        }
        return options.onNull().orElse(null);
    }

    private Boolean parseFromMissingBoolean(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, Boolean.class);
        }
        return options.onMissing().orElse(null);
    }

    private Boolean parseFromStringBoolean(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Helpers.mismatch(parser, Boolean.class);
        }
        if (!options.allowNull()) {
            final Boolean result = Helpers.parseStringAsBoolean(parser, null);
            if (result == null) {
                throw Helpers.mismatch(parser, Boolean.class);
            }
            return result;
        }
        return Helpers.parseStringAsBoolean(parser, options.onNull().orElse(null));
    }
}
