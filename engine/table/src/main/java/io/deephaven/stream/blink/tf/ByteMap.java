package io.deephaven.stream.blink.tf;

import java.util.Objects;
import java.util.function.Function;

class ByteMap<T, R> implements ByteFunction<T> {
    private final Function<T, R> f;
    private final ByteFunction<R> g;

    public ByteMap(Function<T, R> f, ByteFunction<R> g) {
        this.f = Objects.requireNonNull(f);
        this.g = Objects.requireNonNull(g);
    }

    @Override
    public byte applyAsByte(T value) {
        return g.applyAsByte(f.apply(value));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ByteMap<?, ?> byteMap = (ByteMap<?, ?>) o;

        if (!f.equals(byteMap.f)) return false;
        return g.equals(byteMap.g);
    }

    @Override
    public int hashCode() {
        int result = f.hashCode();
        result = 31 * result + g.hashCode();
        return result;
    }
}
