package io.deephaven.stream.blink.tf;

import java.util.Objects;

class MapVisitor<T, R> implements TypedFunction.Visitor<T, TypedFunction<R>> {

    public static <T, R> TypedFunction<R> of(ObjectFunction<R, T> f, TypedFunction<T> g) {
        return g.walk(new MapVisitor<>(f));
    }

    private final ObjectFunction<R, T> f;

    private MapVisitor(ObjectFunction<R, T> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public TypedFunction<R> visit(PrimitiveFunction<T> g) {
        return f.mapPrimitive(g);
    }

    @Override
    public TypedFunction<R> visit(ObjectFunction<T, ?> g) {
        return f.mapObj(g);
    }
}
