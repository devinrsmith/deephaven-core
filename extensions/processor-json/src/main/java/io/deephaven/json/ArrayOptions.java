/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class ArrayOptions extends ValueOptions {

    public static Builder builder() {
        return null;
        // return ImmutableArrayOptions.builder();
    }

    public abstract ValueOptions element();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }


    public interface Builder extends ValueOptions.Builder<ArrayOptions, Builder> {

        Builder element(ValueOptions options);
    }
}
