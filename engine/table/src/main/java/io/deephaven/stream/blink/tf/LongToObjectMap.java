package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.GenericType;
import io.deephaven.stream.blink.tf.LongFunction.LongToObject;

import java.util.Objects;

class LongToObjectMap<T, R> implements ObjectFunction<T, R> {
    private final LongFunction<T> f;
    private final LongToObject<R> g;
    private final GenericType<R> returnType;

    public LongToObjectMap(LongFunction<T> f, LongToObject<R> g, GenericType<R> returnType) {
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
        return g.apply(f.applyAsLong(value));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LongToObjectMap<?, ?> that = (LongToObjectMap<?, ?>) o;

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
