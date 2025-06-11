//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

abstract class JacksonIteratorProviderBase implements JacksonIteratorProvider {

    private final Mixin<?> mixin;
    final ValueProcessor processor;

    JacksonIteratorProviderBase(Mixin<?> mixin) {
        this.mixin = Objects.requireNonNull(mixin);
        this.processor = mixin.processor("<root>");
    }

    @Override
    public final List<Type<?>> outputTypes() {
        return mixin.outputTypes();
    }

    @Override
    public final int outputSize() {
        return mixin.outputSize();
    }

    @Override
    public final List<String> names() {
        return mixin.names();
    }

    @Override
    public final List<String> names(Function<List<String>, String> f) {
        return mixin.names(f);
    }
}
