/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.FloatOptions;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class FloatMixin extends Mixin<FloatOptions> {

    public FloatMixin(FloatOptions options, JsonFactory factory) {
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
        return Stream.of(Type.floatType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new FloatValueProcessor(out.get(0).asWritableFloatChunk(), new Impl());
    }

    private float onNullOrDefault() {
        final Float onNull = options.onNull();
        return onNull != null ? onNull : QueryConstants.NULL_FLOAT;
    }

    private float onMissingOrDefault() {
        final Float onMissing = options.onMissing();
        return onMissing != null ? onMissing : QueryConstants.NULL_FLOAT;
    }

    private float parseFromNumber(JsonParser parser) throws IOException {
        if (!options.allowNumberFloat() && !options.allowNumberInt()) {
            throw Helpers.mismatch(parser, float.class);
        }
        return Helpers.parseNumberAsFloat(parser);
    }

    private float parseFromString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Helpers.mismatch(parser, float.class);
        }
        return Helpers.parseStringAsFloat(parser);
    }

    private float parseFromNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, float.class);
        }
        return onNullOrDefault();
    }

    private float parseMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, float.class);
        }
        return onMissingOrDefault();
    }

    class Impl implements FloatValueProcessor.ToFloat {
        @Override
        public float parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_NUMBER_INT:
                case VALUE_NUMBER_FLOAT:
                    return parseFromNumber(parser);
                case VALUE_STRING:
                    return parseFromString(parser);
                case VALUE_NULL:
                    return parseFromNull(parser);
            }
            throw Helpers.mismatch(parser, float.class);
        }

        @Override
        public float parseMissing(JsonParser parser) throws IOException {
            return FloatMixin.this.parseMissing(parser);
        }
    }
}
