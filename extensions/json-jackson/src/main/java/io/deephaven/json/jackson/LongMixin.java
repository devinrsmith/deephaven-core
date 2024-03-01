/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.LongOptions;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class LongMixin extends Mixin<LongOptions> {

    public LongMixin(LongOptions options, JsonFactory config) {
        super(config, options);
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
        return Stream.of(Type.longType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new LongValueProcessor(out.get(0).asWritableLongChunk(), new Impl());
    }

    private long parseFromInt(JsonParser parser) throws IOException {
        if (!options.allowNumberInt()) {
            throw Helpers.mismatch(parser, long.class);
        }
        return Helpers.parseIntAsLong(parser);
    }

    private long parseFromDecimal(JsonParser parser) throws IOException {
        if (!options.allowNumberFloat()) {
            throw Helpers.mismatch(parser, long.class);
        }
        // todo: allow caller to configure between lossy long and truncated long
        return Helpers.parseDecimalAsLossyLong(parser);
    }

    private long parseFromString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Helpers.mismatch(parser, long.class);
        }
        return options.allowDecimal()
                // todo: allow caller to configure between lossy long and truncated long
                // ? Helpers.parseDecimalStringAsLossyLong(parser)
                ? Helpers.parseDecimalStringAsTruncatedLong(parser)
                : Helpers.parseStringAsLong(parser);
    }

    private long parseFromNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, long.class);
        }
        return options.onNull().orElse(QueryConstants.NULL_LONG);
    }

    private long parseMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, long.class);
        }
        return options.onMissing().orElse(QueryConstants.NULL_LONG);
    }

    private class Impl implements LongValueProcessor.ToLong {
        @Override
        public long parseValue(JsonParser parser) throws IOException {
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
            throw Helpers.mismatch(parser, long.class);
        }

        @Override
        public long parseMissing(JsonParser parser) throws IOException {
            return LongMixin.this.parseMissing(parser);
        }
    }
}
