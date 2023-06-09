package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.GenericType;

import java.util.Objects;
import java.util.function.Function;

final class ObjectFunctionImpl<T, R> implements ObjectFunction<T, R> {
    private final Function<T, R> f;
    private final GenericType<R> returnType;

    ObjectFunctionImpl(Function<T, R> f, GenericType<R> returnType) {
        this.f = Objects.requireNonNull(f);
        this.returnType = Objects.requireNonNull(returnType);
    }

    @Override
    public GenericType<R> returnType() {
        return returnType;
    }

    @Override
    public R apply(T value) {
        return f.apply(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ObjectFunctionImpl<?, ?> that = (ObjectFunctionImpl<?, ?>) o;

        if (!returnType.equals(that.returnType)) return false;
        return f.equals(that.f);
    }

    @Override
    public int hashCode() {
        int result = returnType.hashCode();
        result = 31 * result + f.hashCode();
        return result;
    }
}
