package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.GenericType;
import io.deephaven.stream.blink.tf.ShortFunction.ShortToObject;

import java.util.Objects;

class ShortToObjectMap<T, R> implements ObjectFunction<T, R> {
    private final ShortFunction<T> f;
    private final ShortToObject<R> g;
    private final GenericType<R> returnType;

    public ShortToObjectMap(ShortFunction<T> f, ShortToObject<R> g, GenericType<R> returnType) {
        this.f = Objects.requireNonNull(f);
        this.g = Objects.requireNonNull(g);
        this.returnType = Objects.requireNonNull(returnType);
    }

    @Override
    public GenericType<R> returnType() {
        return returnType;
    }

    @Override
    public R apply(T value) {
        return g.apply(f.applyAsShort(value));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShortToObjectMap<?, ?> that = (ShortToObjectMap<?, ?>) o;

        if (!f.equals(that.f)) return false;
        if (!g.equals(that.g)) return false;
        return returnType.equals(that.returnType);
    }

    @Override
    public int hashCode() {
        int result = f.hashCode();
        result = 31 * result + g.hashCode();
        result = 31 * result + returnType.hashCode();
        return result;
    }
}
