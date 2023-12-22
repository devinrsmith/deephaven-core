/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import javax.annotation.Nullable;

public abstract class SingleValueOptions<T, F extends Function> extends ValueOptions {

    abstract F onValue();

    @Nullable
    public abstract T onMissing();

    interface Builder<T, F extends Function, V extends SingleValueOptions<T, F>, B extends Builder<T, F, V, B>>
            extends ValueOptions.Builder<V, B> {

        B onValue(F onValue);

        B onMissing(T onMissing);
    }
}
