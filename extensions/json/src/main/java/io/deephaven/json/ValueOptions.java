/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public abstract class ValueOptions {

    public static ValueOptions skip() {
        return new SkipOpts();
    }

    public abstract boolean allowNull();

    public abstract boolean allowMissing();

    public final ArrayOptions array() {
        return ArrayOptions.builder()
                .element(this)
                .build();
    }

    // todo: what about multivariate?
    abstract Stream<Type<?>> outputTypes();

    abstract Set<JsonToken> startTokens();

    abstract ValueProcessor processor(String context, List<WritableChunk<?>> out);

    final int numColumns() {
        return (int) outputTypes().count();
    }

    interface Builder<V extends ValueOptions, B extends Builder<V, B>> {
        B allowNull(boolean allowNull);

        B allowMissing(boolean allowMissing);

        V build();
    }
}
