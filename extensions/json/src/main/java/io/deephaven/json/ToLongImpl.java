/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.json.Function.ToLong;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

@Immutable
@BuildableStyle
public abstract class ToLongImpl extends PerJsonType<ToLong> implements ToLong {

    public static Builder builder() {
        return ImmutableToLongImpl.builder();
    }

    private static final ToLong STANDARD = builder()
            .onNumberInt(Parser.LONG_VALUE)
            .onNull(DhNull.DH_NULL)
            .build();

    private static final ToLong STRICT = builder()
            .onNumberInt(Parser.LONG_VALUE)
            .build();

    private static final ToLong LENIENT = builder()
            .onNumberInt(Parser.LONG_VALUE)
            .onNumberFloat(Parser.LONG_VALUE)
            .onNull(DhNull.DH_NULL)
            .onObject(DhNull.DH_NULL)
            .onArray(DhNull.DH_NULL)
            .onString(DhNull.DH_NULL)
            .onBoolean(DhNull.DH_NULL)
            .build();

    public static ToLong standard() {
        return STANDARD;
    }

    public static ToLong strict() {
        return STRICT;
    }

    public static ToLong lenient() {
        return LENIENT;
    }

    /**
     * The parser to use for {@link JsonToken#VALUE_NUMBER_INT} values.
     *
     * @return the parser
     */
    @Nullable
    public abstract ToLong onNumberInt();

    /**
     * The parser to use for {@link JsonToken#VALUE_NUMBER_FLOAT} values.
     *
     * @return the parser
     */
    @Nullable
    public abstract ToLong onNumberFloat();

    /**
     * The parser to use for {@link JsonToken#VALUE_NULL} values.
     *
     * @return the parser
     */
    @Nullable
    public abstract ToLong onNull();

    /**
     * The parser to use for {@link JsonToken#VALUE_STRING} values.
     *
     * @return the parser
     */
    @Nullable
    public abstract ToLong onString();

    /**
     * The parser to use for {@link JsonToken#START_OBJECT} values.
     *
     * @return the parser
     */
    @Nullable
    public abstract ToLong onObject();

    /**
     * The parser to use for {@link JsonToken#START_ARRAY} values.
     *
     * @return the parser
     */
    @Nullable
    public abstract ToLong onArray();

    /**
     * The parser to use for {@link JsonToken#VALUE_TRUE} or {@link JsonToken#VALUE_FALSE} values.
     *
     * @return the parser
     */
    @Nullable
    public abstract ToLong onBoolean();

    /**
     * Delegates to the appropriate parser based on {@link JsonParser#currentToken()}.
     *
     * @param parser the parser
     * @return the double result
     * @throws IOException if a low-level IO exception occurs
     */
    @Override
    public final long applyAsLong(JsonParser parser) throws IOException {
        final JsonToken token = Objects.requireNonNull(parser.currentToken());
        final ToLong delegate = onToken(token);
        if (delegate == null) {
            throw MismatchedInputException.from(parser, double.class,
                    String.format("Not expecting token '%s', %s", token, this));
        }
        return delegate.applyAsLong(parser);
    }

    public interface Builder extends PerJsonType.Builder<ToLong, Builder> {

    }
}
