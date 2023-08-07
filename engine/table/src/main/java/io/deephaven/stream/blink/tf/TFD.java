package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.Type;

import java.util.function.Function;

public class TFD<T> implements TypedFunction<T> {

    protected TypedFunction<T> delegate();

    @Override
    public Type<?> returnType() {
        return delegate().returnType();
    }

    @Override
    public <R> R walk(Visitor<T, R> visitor) {
        return delegate().walk(visitor);
    }

    @Override
    public TypedFunction<T> mapInput(Function<T, T> f) {
        return delegate().mapInput(f);
    }
}
