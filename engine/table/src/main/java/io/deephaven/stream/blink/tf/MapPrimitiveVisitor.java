package io.deephaven.stream.blink.tf;

import java.util.Objects;

class MapPrimitiveVisitor<T, R> implements PrimitiveFunction.Visitor<T, PrimitiveFunction<R>> {

    public static <T, R> PrimitiveFunction<R> of(ObjectFunction<R, T> f, PrimitiveFunction<T> g) {
        return g.walk(new MapPrimitiveVisitor<>(f));
    }

    private final ObjectFunction<R, T> f;

    private MapPrimitiveVisitor(ObjectFunction<R, T> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public PrimitiveFunction<R> visit(BooleanFunction<T> g) {
        return f.mapBoolean(g);
    }

    @Override
    public PrimitiveFunction<R> visit(CharFunction<T> g) {
        return f.mapChar(g);
    }

    @Override
    public PrimitiveFunction<R> visit(ByteFunction<T> g) {
        return f.mapByte(g);
    }

    @Override
    public PrimitiveFunction<R> visit(ShortFunction<T> g) {
        return f.mapShort(g);
    }

    @Override
    public PrimitiveFunction<R> visit(IntFunction<T> g) {
        return f.mapInt(g);
    }

    @Override
    public PrimitiveFunction<R> visit(LongFunction<T> g) {
        return f.mapLong(g);
    }

    @Override
    public PrimitiveFunction<R> visit(FloatFunction<T> g) {
        return f.mapFloat(g);
    }

    @Override
    public PrimitiveFunction<R> visit(DoubleFunction<T> g) {
        return f.mapDouble(g);
    }
}
