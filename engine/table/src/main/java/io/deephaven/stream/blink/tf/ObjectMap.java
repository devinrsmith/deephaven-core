package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.GenericType;

import java.util.Objects;
import java.util.function.Function;

class ObjectMap<T, R, Z> implements ObjectFunction<T, Z> {
    private final Function<T, R> f;
    private final ObjectFunction<R, Z> g;

    public ObjectMap(Function<T, R> f, ObjectFunction<R, Z> g) {
        this.f = Objects.requireNonNull(f);
        this.g = Objects.requireNonNull(g);
    }

    @Override
    public GenericType<Z> returnType() {
        return g.returnType();
    }

    @Override
    public Z apply(T value) {
        return g.apply(f.apply(value));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ObjectMap<?, ?, ?> objectMap = (ObjectMap<?, ?, ?>) o;

        if (!f.equals(objectMap.f)) return false;
        return g.equals(objectMap.g);
    }

    @Override
    public int hashCode() {
        int result = f.hashCode();
        result = 31 * result + g.hashCode();
        return result;
    }
}
