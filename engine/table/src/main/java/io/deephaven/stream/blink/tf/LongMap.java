package io.deephaven.stream.blink.tf;

import java.util.Objects;
import java.util.function.Function;

class LongMap<T, R> implements LongFunction<T> {
    private final Function<T, R> f;
    private final LongFunction<R> g;

    public LongMap(Function<T, R> f, LongFunction<R> g) {
        this.f = Objects.requireNonNull(f);
        this.g = Objects.requireNonNull(g);
    }

    @Override
    public long applyAsLong(T value) {
        return g.applyAsLong(f.apply(value));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LongMap<?, ?> longMap = (LongMap<?, ?>) o;

        if (!f.equals(longMap.f)) return false;
        return g.equals(longMap.g);
    }

    @Override
    public int hashCode() {
        int result = f.hashCode();
        result = 31 * result + g.hashCode();
        return result;
    }
}
