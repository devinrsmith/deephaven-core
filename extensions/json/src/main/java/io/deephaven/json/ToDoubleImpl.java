/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.json.Function.ToDouble;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

@Immutable
@BuildableStyle
public abstract class ToDoubleImpl extends PerJsonType<ToDouble> implements ToDouble {

    public static Builder builder() {
        return ImmutableToDoubleImpl.builder();
    }

    private static final ToDouble STANDARD = builder()
            .onNumberFloat(Parser.DOUBLE_VALUE)
            .onNumberInt(Parser.DOUBLE_VALUE)
            .onNull(DhNull.DH_NULL)
            .build();

    private static final ToDouble STRICT = builder()
            .onNumberFloat(Parser.DOUBLE_VALUE)
            .onNumberInt(Parser.DOUBLE_VALUE)
            .build();

    private static final ToDouble LENIENT = builder()
            .onNumberFloat(Parser.DOUBLE_VALUE)
            .onNumberInt(Parser.DOUBLE_VALUE)
            .onNull(DhNull.DH_NULL)
            .onObject(DhNull.DH_NULL)
            .onArray(DhNull.DH_NULL)
            .onString(DhNull.DH_NULL)
            .onBoolean(DhNull.DH_NULL)
            .build();

    public static ToDouble standard() {
        return STANDARD;
    }

    public static ToDouble strict() {
        return STRICT;
    }

    public static ToDouble lenient() {
        return LENIENT;
    }

    /**
     * The parser to use for {@link JsonToken#VALUE_NUMBER_INT} values.
     *
     * @return the parser
     */
    @Nullable
    public abstract ToDouble onNumberInt();

    /**
     * The parser to use for {@link JsonToken#VALUE_NUMBER_FLOAT} values.
     *
     * @return the parser
     */
    @Nullable
    public abstract ToDouble onNumberFloat();

    /**
     * The parser to use for {@link JsonToken#VALUE_NULL} values.
     *
     * @return the parser
     */
    @Nullable
    public abstract ToDouble onNull();

    /**
     * The parser to use for {@link JsonToken#VALUE_STRING} values.
     *
     * @return the parser
     */
    @Nullable
    public abstract ToDouble onString();

    /**
     * The parser to use for {@link JsonToken#START_OBJECT} values.
     *
     * @return the parser
     */
    @Nullable
    public abstract ToDouble onObject();

    /**
     * The parser to use for {@link JsonToken#START_ARRAY} values.
     *
     * @return the parser
     */
    @Nullable
    public abstract ToDouble onArray();

    /**
     * The parser to use for {@link JsonToken#VALUE_TRUE} or {@link JsonToken#VALUE_FALSE} values.
     *
     * @return the parser
     */
    @Nullable
    public abstract ToDouble onBoolean();

    /**
     * Delegates to the appropriate parser based on {@link JsonParser#currentToken()}.
     *
     * @param parser the parser
     * @return the double result
     * @throws IOException if a low-level IO exception occurs
     */
    @Override
    public final double applyAsDouble(JsonParser parser) throws IOException {
        final JsonToken token = Objects.requireNonNull(parser.currentToken());
        final ToDouble delegate = onToken(token);
        if (delegate == null) {
            throw MismatchedInputException.from(parser, double.class,
                    String.format("Not expecting token '%s', %s", token, this));
        }
        return delegate.applyAsDouble(parser);
    }

    public interface Builder extends PerJsonType.Builder<ToDouble, Builder> {

    }
}
