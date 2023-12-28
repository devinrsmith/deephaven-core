/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Default;

import java.util.List;
import java.util.stream.Stream;

public abstract class ValueOptions {


    @Default
    public boolean allowNull() {
        return true;
    }

    @Default
    public boolean allowMissing() {
        return true;
    }

    public final ArrayOptions toArrayOptions() {
        return null;
        // return ArrayOptions.builder()
        // .element(this)
        // .build();
    }

    // todo: what about multivariate?
    abstract Stream<Type<?>> outputTypes();

    abstract ValueProcessor processor(String context, List<WritableChunk<?>> out);

    // for nested / typedescr cases
    ValueOptions withMissingSupport() {
        if (allowMissing()) {
            return this;
        }
        throw new UnsupportedOperationException(); // todo
    }

    final int numColumns() {
        return (int) outputTypes().count();
    }

    public interface Builder<V extends ValueOptions, B extends Builder<V, B>> {

        B allowNull(boolean allowNull);

        B allowMissing(boolean allowMissing);

        V build();
    }
}
