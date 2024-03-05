/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.ArrayUtil;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ShortOptions;
import io.deephaven.json.jackson.ShortValueProcessor.ToShort;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.deephaven.util.type.ArrayTypeUtils.EMPTY_SHORT_ARRAY;

final class ShortMixin extends Mixin<ShortOptions> implements ToShort {
    public ShortMixin(ShortOptions options, JsonFactory factory) {
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
        return Stream.of(Type.shortType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new ShortValueProcessor(out.get(0).asWritableShortChunk(), this);
    }

    @Override
    public short parseValue(JsonParser parser) throws IOException {
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
    public short parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    ArrayProcessor arrayProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {

        return new ShortArrayProcessorImpl(out.get(0).asWritableObjectChunk()::add, allowMissing, allowNull);
    }

    final class ShortArrayProcessorImpl extends ArrayProcessorBase<short[]> {

        public ShortArrayProcessorImpl(Consumer<? super short[]> consumer, boolean allowMissing, boolean allowNull) {
            super(consumer, allowMissing, allowNull, null, null);
        }

        @Override
        public ShortArrayContext newContext() {
            return new ShortArrayContext();
        }

        final class ShortArrayContext extends ArrayContextBase {
            private short[] arr = EMPTY_SHORT_ARRAY;
            private int len = 0;

            @Override
            public void processElement(JsonParser parser, int index) throws IOException {
                if (index != len) {
                    throw new IllegalStateException();
                }
                arr = ArrayUtil.put(arr, len, ShortMixin.this.parseValue(parser));
                ++len;
            }

            @Override
            public void processElementMissing(JsonParser parser, int index) throws IOException {
                if (index != len) {
                    throw new IllegalStateException();
                }
                arr = ArrayUtil.put(arr, len, ShortMixin.this.parseMissing(parser));
                ++len;
            }

            @Override
            public short[] onDone(int length) {
                if (length != len) {
                    throw new IllegalStateException();
                }
                return arr.length == len ? arr : Arrays.copyOf(arr, len);
            }
        }
    }

    private short parseFromInt(JsonParser parser) throws IOException {
        if (!options.allowNumberInt()) {
            throw Helpers.mismatch(parser, short.class);
        }
        return Helpers.parseIntAsShort(parser);
    }

    private short parseFromDecimal(JsonParser parser) throws IOException {
        if (!options.allowDecimal()) {
            throw Helpers.mismatch(parser, short.class);
        }
        return Helpers.parseDecimalAsTruncatedShort(parser);
    }

    private short parseFromString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Helpers.mismatch(parser, short.class);
        }
        return options.allowDecimal()
                ? Helpers.parseDecimalStringAsTruncatedShort(parser)
                : Helpers.parseStringAsShort(parser);
    }

    private short parseFromNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, short.class);
        }
        return options.onNull().orElse(QueryConstants.NULL_SHORT);
    }

    private short parseFromMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, short.class);
        }
        return options.onMissing().orElse(QueryConstants.NULL_SHORT);
    }
}
