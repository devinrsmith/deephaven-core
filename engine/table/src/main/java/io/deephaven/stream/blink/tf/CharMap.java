package io.deephaven.stream.blink.tf;

import java.util.Objects;
import java.util.function.Function;

class CharMap<T, R> implements CharFunction<T> {
    private final Function<T, R> f;
    private final CharFunction<R> g;

    public CharMap(Function<T, R> f, CharFunction<R> g) {
        this.f = Objects.requireNonNull(f);
        this.g = Objects.requireNonNull(g);
    }

    @Override
    public char applyAsChar(T value) {
        return g.applyAsChar(f.apply(value));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CharMap<?, ?> charMap = (CharMap<?, ?>) o;

        if (!f.equals(charMap.f)) return false;
        return g.equals(charMap.g);
    }

    @Override
    public int hashCode() {
        int result = f.hashCode();
        result = 31 * result + g.hashCode();
        return result;
    }
}
