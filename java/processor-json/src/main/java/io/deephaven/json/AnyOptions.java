/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * Note: TreeNode should not be considered part of the public API. Implementation may change.
 */
@Immutable
@SimpleStyle
public abstract class AnyOptions extends ValueOptions {

    public static AnyOptions of() {
        return ImmutableAnyOptions.of();
    }

    @Override
    public final boolean allowNull() {
        return true;
    }

    @Override
    public final boolean allowMissing() {
        return true;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
