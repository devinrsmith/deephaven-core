/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class StringOptions extends ValueOptions {
    private static final StringOptions STANDARD = builder().build();
    private static final StringOptions STRICT = builder()
            .allowNull(false)
            .allowMissing(false)
            .build();
    private static final StringOptions LENIENT = builder()
            .allowNumberInt(true)
            .allowNumberFloat(true)
            .allowBoolean(true)
            .build();

    public static Builder builder() {
        return ImmutableStringOptions.builder();
    }

    public static StringOptions standard() {
        return STANDARD;
    }

    public static StringOptions strict() {
        return STRICT;
    }

    public static StringOptions lenient() {
        return LENIENT;
    }

    @Default
    public boolean allowString() {
        return true;
    }

    @Default
    public boolean allowNumberInt() {
        return false;
    }

    @Default
    public boolean allowNumberFloat() {
        return false;
    }

    @Default
    public boolean allowBoolean() {
        return false;
    }

    public abstract Optional<String> onNull();

    public abstract Optional<String> onMissing();

    public final SkipOptions skip() {
        return SkipOptions.builder()
                .allowString(allowString())
                .allowNumberInt(allowNumberInt())
                .allowNumberFloat(allowNumberFloat())
                .allowBoolean(allowBoolean())
                .allowNull(allowNull())
                .allowMissing(allowMissing())
                .build();
    }

    public interface Builder extends ValueOptions.Builder<StringOptions, Builder> {
        Builder allowString(boolean allowString);

        Builder allowNumberInt(boolean allowNumberInt);

        Builder allowNumberFloat(boolean allowNumberFloat);

        Builder allowBoolean(boolean allowBoolean);

        Builder onNull(String onNull);

        Builder onMissing(String onMissing);
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.stringType());
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new ObjectValueProcessor<>(out.get(0).asWritableObjectChunk(), new Impl());
    }

    @Check
    final void checkOnNull() {
        if (!allowNull() && onNull().isPresent()) {
            throw new IllegalArgumentException();
        }
    }

    @Check
    final void checkOnMissing() {
        if (!allowMissing() && onMissing().isPresent()) {
            throw new IllegalArgumentException();
        }
    }

    private String parseString(JsonParser parser) throws IOException {
        if (!allowString()) {
            throw Helpers.mismatch(parser, String.class);
        }
        return parser.getText();
    }

    private String parseNumberInt(JsonParser parser) throws IOException {
        if (!allowNumberInt()) {
            throw Helpers.mismatch(parser, String.class);
        }
        return parser.getText();
    }

    private String parseNumberFloat(JsonParser parser) throws IOException {
        if (!allowNumberFloat()) {
            throw Helpers.mismatch(parser, String.class);
        }
        return parser.getText();
    }

    private String parseBoolean(JsonParser parser) throws IOException {
        if (!allowBoolean()) {
            throw Helpers.mismatch(parser, String.class);
        }
        return parser.getText();
    }

    private String parseNull(JsonParser parser) throws MismatchedInputException {
        if (!allowNull()) {
            throw Helpers.mismatch(parser, String.class);
        }
        return onNull().orElse(null);
    }

    private String parseMissing(JsonParser parser) throws MismatchedInputException {
        if (!allowMissing()) {
            throw Helpers.mismatchMissing(parser, String.class);
        }
        return onMissing().orElse(null);
    }

    private class Impl implements ObjectValueProcessor.ToObject<String> {
        @Override
        public String parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_STRING:
                    return parseString(parser);
                case VALUE_NUMBER_INT:
                    return parseNumberInt(parser);
                case VALUE_NUMBER_FLOAT:
                    return parseNumberFloat(parser);
                case VALUE_TRUE:
                case VALUE_FALSE:
                    return parseBoolean(parser);
                case VALUE_NULL:
                    return parseNull(parser);
            }
            throw Helpers.mismatch(parser, String.class);
        }

        @Override
        public String parseMissing(JsonParser parser) throws IOException {
            return StringOptions.this.parseMissing(parser);
        }
    }
}
