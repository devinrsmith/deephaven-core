/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class SkipOptions extends ValueOptions {

    public static Builder builder() {
        return ImmutableSkipOptions.builder();
    }

    public static SkipOptions numberInt() {
        return builder().allowNumberInt(true).build();
    }

    public static SkipOptions numberFloat() {
        return builder().allowNumberFloat(true).build();
    }

    public static SkipOptions string() {
        return builder().allowString(true).build();
    }

    public static SkipOptions _boolean() {
        return builder().allowBoolean(true).build();
    }

    public static SkipOptions object() {
        return builder().allowObject(true).build();
    }

    public static SkipOptions array() {
        return builder().allowArray(true).build();
    }

    public static SkipOptions lenient() {
        return builder()
                .allowNumberInt(true)
                .allowNumberFloat(true)
                .allowString(true)
                .allowBoolean(true)
                .allowObject(true)
                .allowArray(true)
                .allowNull(true)
                .allowMissing(true)
                .build();
    }

    public abstract boolean allowNumberInt();

    public abstract boolean allowNumberFloat();

    public abstract boolean allowString();

    public abstract boolean allowBoolean();

    public abstract boolean allowObject();

    public abstract boolean allowArray();

    public interface Builder extends ValueOptions.Builder<SkipOptions, Builder> {
        Builder allowNumberInt(boolean allowNumberInt);

        Builder allowNumberFloat(boolean allowNumberInt);

        Builder allowString(boolean allowString);

        Builder allowBoolean(boolean allowBoolean);

        Builder allowObject(boolean allowObject);

        Builder allowArray(boolean allowArray);
    }

    @Override
    final int outputCount() {
        return 0;
    }

    @Override
    final Stream<List<String>> paths() {
        return Stream.empty();
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.empty();
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new Impl();
    }

    class Impl implements ValueProcessor {
        @Override
        public void processCurrentValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case START_OBJECT:
                    if (!allowObject()) {
                        throw Helpers.mismatch(parser, void.class);
                    }
                    parser.skipChildren();
                    break;
                case START_ARRAY:
                    if (!allowArray()) {
                        throw Helpers.mismatch(parser, void.class);
                    }
                    parser.skipChildren();
                    break;
                case VALUE_STRING:
                    if (!allowString()) {
                        throw Helpers.mismatch(parser, void.class);
                    }
                    break;
                case VALUE_NUMBER_INT:
                    if (!allowNumberInt()) {
                        throw Helpers.mismatch(parser, void.class);
                    }
                    break;
                case VALUE_NUMBER_FLOAT:
                    if (!allowNumberFloat()) {
                        throw Helpers.mismatch(parser, void.class);
                    }
                    break;
                case VALUE_TRUE:
                case VALUE_FALSE:
                    if (!allowBoolean()) {
                        throw Helpers.mismatch(parser, void.class);
                    }
                    break;
                case VALUE_NULL:
                    if (!allowNull()) {
                        throw Helpers.mismatch(parser, void.class);
                    }
                    break;
                default:
                    throw Helpers.mismatch(parser, void.class);
            }
        }

        @Override
        public void processMissing(JsonParser parser) throws IOException {
            if (!allowMissing()) {
                throw Helpers.mismatchMissing(parser, void.class);
            }
        }
    }
}
