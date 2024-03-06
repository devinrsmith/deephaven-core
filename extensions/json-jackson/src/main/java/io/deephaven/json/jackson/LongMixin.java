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

final class LongMixin extends Mixin<LongOptions> implements LongValueProcessor.ToLong {

    public LongMixin(LongOptions options, JsonFactory config) {
        super(config, options);
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
        return Stream.of(Type.longType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return LongValueProcessor.of(out.get(0).asWritableLongChunk(), this);
    }

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
        throw Parsing.mismatch(parser, long.class);
    }

    @Override
    public long parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    ArrayProcessor arrayProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        return new LongArrayProcessorImpl(this, allowMissing, allowNull, out.get(0).asWritableObjectChunk()::add);
    }

    private long parseFromInt(JsonParser parser) throws IOException {
        if (!options.allowNumberInt()) {
            throw Parsing.mismatch(parser, long.class);
        }
        return Parsing.parseIntAsLong(parser);
    }

    private long parseFromDecimal(JsonParser parser) throws IOException {
        if (!options.allowDecimal()) {
            throw Parsing.mismatch(parser, long.class);
        }
        // todo: allow caller to configure between lossy long and truncated long
        return Parsing.parseDecimalAsLossyLong(parser);
    }

    private long parseFromString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Parsing.mismatch(parser, long.class);
        }
        return options.allowDecimal()
                // todo: allow caller to configure between lossy long and truncated long
                // ? Helpers.parseDecimalStringAsLossyLong(parser)
                ? Parsing.parseDecimalStringAsTruncatedLong(parser)
                : Parsing.parseStringAsLong(parser);
    }

    private long parseFromNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Parsing.mismatch(parser, long.class);
        }
        return options.onNull().orElse(QueryConstants.NULL_LONG);
    }

    private long parseFromMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Parsing.mismatchMissing(parser, long.class);
        }
        return options.onMissing().orElse(QueryConstants.NULL_LONG);
    }
}
