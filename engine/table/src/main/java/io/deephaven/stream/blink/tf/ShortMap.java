package io.deephaven.stream.blink.tf;

import java.util.Objects;
import java.util.function.Function;

class ShortMap<T, R> implements ShortFunction<T> {
    private final Function<T, R> f;
    private final ShortFunction<R> g;

    public ShortMap(Function<T, R> f, ShortFunction<R> g) {
        this.f = Objects.requireNonNull(f);
        this.g = Objects.requireNonNull(g);
    }

    @Override
    public short applyAsShort(T value) {
        return g.applyAsShort(f.apply(value));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShortMap<?, ?> shortMap = (ShortMap<?, ?>) o;

        if (!f.equals(shortMap.f)) return false;
        return g.equals(shortMap.g);
    }

    @Override
    public int hashCode() {
        int result = f.hashCode();
        result = 31 * result + g.hashCode();
        return result;
    }
}
