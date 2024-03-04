/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.ArrayUtil;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ArrayOptions;
import io.deephaven.json.LongOptions;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

final class LongMixin extends Mixin<LongOptions> implements  LongValueProcessor.ToLong {

    private static final long[] EMPTY_LONG_ARRAY = new long[0];

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
        return LongValueProcessor.of(out.get(0).asWritableLongChunk(), this);
    }

    @Override
    ArrayProcessor arrayProcessor(ArrayOptions options, List<WritableChunk<?>> out) {
        if (options.element() != options()) {
            throw new IllegalStateException();
        }
        return new LongArrayProcessorImpl(options, out.get(0).asWritableObjectChunk()::add);
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
        throw Helpers.mismatch(parser, long.class);
    }

    @Override
    public long parseMissing(JsonParser parser) throws IOException {
        return LongMixin.this.parseFromMissing(parser);
    }

    final class LongArrayProcessorImpl extends ArrayProcessorBase<long[]> {

        public LongArrayProcessorImpl(ArrayOptions options, Consumer<? super long[]> consumer) {
            super(options, consumer, null, null);
        }

        @Override
        public LongArrayContext newContext() {
            return new LongArrayContext();
        }

        final class LongArrayContext extends ArrayContextBase {
            private long[] arr = EMPTY_LONG_ARRAY;
            private int len = 0;

            @Override
            public void processElement(int ix, JsonParser parser) throws IOException {
                if (ix != len) {
                    throw new IllegalStateException();
                }
                arr = ArrayUtil.put(arr, len, LongMixin.this.parseValue(parser));
                ++len;
            }

            @Override
            public long[] onDone() {
                return arr.length == len ? arr : Arrays.copyOf(arr, len);
            }
        }
    }

    private long parseFromInt(JsonParser parser) throws IOException {
        if (!options.allowNumberInt()) {
            throw Helpers.mismatch(parser, long.class);
        }
        return Helpers.parseIntAsLong(parser);
    }

    private long parseFromDecimal(JsonParser parser) throws IOException {
        if (!options.allowDecimal()) {
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

    private long parseFromMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, long.class);
        }
        return options.onMissing().orElse(QueryConstants.NULL_LONG);
    }
}
