/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.json.Functions.ToLong;
import io.deephaven.util.QueryConstants;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.io.IOException;

@Immutable
@BuildableStyle
public abstract class ToLongImpl extends PerJsonType<ToLong> implements ToLong {

    public static Builder builder() {
        return ImmutableToLongImpl.builder();
    }

    @Default
    @Nullable
    public ToLong onNumberInt() {
        return Plain.LONG_VALUE;
    }

    @Default
    @Nullable
    public ToLong onNull() {
        return x -> QueryConstants.NULL_LONG;
    }

    @Nullable
    public abstract ToLong onNumberFloat();

    @Nullable
    public abstract ToLong onString();

    @Nullable
    public abstract ToLong onObject();

    @Nullable
    public abstract ToLong onArray();

    @Nullable
    public abstract ToLong onBoolean();

    @Override
    public final long applyAsLong(JsonParser parser) throws IOException {
        final JsonToken token = parser.currentToken();
        final ToLong delegate = onToken(token);
        if (delegate == null) {
            throw new IllegalStateException(String.format("[%s]: Unexpected token '%s'", "todo context", token));
        }
        return delegate.applyAsLong(parser);
    }

    public interface Builder extends PerJsonType.Builder<ToLong, Builder> {

    }
}
