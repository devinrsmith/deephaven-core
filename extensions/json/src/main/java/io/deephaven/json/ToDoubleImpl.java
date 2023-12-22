/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.json.Functions.ToDouble;
import io.deephaven.util.QueryConstants;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.io.IOException;

@Immutable
@BuildableStyle
public abstract class ToDoubleImpl extends PerJsonType<ToDouble> implements ToDouble {

    public static Builder builder() {
        return ImmutableToDoubleImpl.builder();
    }

    @Default
    @Nullable
    public ToDouble onNumberInt() {
        return Plain.DOUBLE_VALUE;
    }

    @Default
    @Nullable
    public ToDouble onNumberFloat() {
        return Plain.DOUBLE_VALUE;
    }

    @Default
    @Nullable
    public ToDouble onNull() {
        return x -> QueryConstants.NULL_DOUBLE;
    }

    @Nullable
    public abstract ToDouble onString();

    @Nullable
    public abstract ToDouble onObject();

    @Nullable
    public abstract ToDouble onArray();

    @Nullable
    public abstract ToDouble onBoolean();

    @Override
    public final double applyAsDouble(JsonParser parser) throws IOException {
        final JsonToken token = parser.currentToken();
        final ToDouble delegate = onToken(token);
        if (delegate == null) {
            throw new IllegalStateException(String.format("[%s]: Unexpected token '%s'", "todo context", token));
        }
        return delegate.applyAsDouble(parser);
    }

    public interface Builder extends PerJsonType.Builder<ToDouble, Builder> {

    }
}
