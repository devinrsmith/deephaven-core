package io.deephaven.blink;

import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.TypedFunction;

import java.util.Objects;
import java.util.function.Function;

public class ModifiedTypeFunction<T> implements TypedFunction<T> {
    private final Function<T, T> first;
    private final TypedFunction<T> delegate;

    public ModifiedTypeFunction(Function<T, T> first, TypedFunction<T> delegate) {
        this.first = Objects.requireNonNull(first);
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public Type<?> returnType() {
        return delegate.returnType();
    }

    @Override
    public <V> V walk(Visitor<T, V> visitor) {
        return delegate.walk(new ModifiedInputVisitor<>(first, visitor));
    }
}
