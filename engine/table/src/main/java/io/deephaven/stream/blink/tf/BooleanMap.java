package io.deephaven.stream.blink.tf;

import java.util.Objects;
import java.util.function.Function;

class BooleanMap<T, R> implements BooleanFunction<T> {
    private final Function<T, R> f;
    private final BooleanFunction<R> g;

    public BooleanMap(Function<T, R> f, BooleanFunction<R> g) {
        this.f = Objects.requireNonNull(f);
        this.g = Objects.requireNonNull(g);
    }

    @Override
    public boolean test(T value) {
        return g.test(f.apply(value));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BooleanMap<?, ?> that = (BooleanMap<?, ?>) o;
        if (!f.equals(that.f)) return false;
        return g.equals(that.g);
    }

    @Override
    public int hashCode() {
        int result = f.hashCode();
        result = 31 * result + g.hashCode();
        return result;
    }
}
