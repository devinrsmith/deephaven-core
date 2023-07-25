package io.deephaven.stream.blink.tf;

import java.util.Objects;

public class MapVisitor<T, R>
        implements TypedFunction.Visitor<T, TypedFunction<R>>, PrimitiveFunction.Visitor<T, TypedFunction<R>> {

    public static <T, R> TypedFunction<R> of(ObjectFunction<R, T> objectFunction, TypedFunction<T> tf) {
        return tf.walk(new MapVisitor<>(objectFunction));
    }

    private final ObjectFunction<R, T> obj;

    private MapVisitor(ObjectFunction<R, T> objectFunction) {
        this.obj = Objects.requireNonNull(objectFunction);
    }

    @Override
    public TypedFunction<R> visit(PrimitiveFunction<T> f) {
        return f.walk((PrimitiveFunction.Visitor<T, TypedFunction<R>>) this);
    }

    @Override
    public TypedFunction<R> visit(ObjectFunction<T, ?> f) {
        return obj.mapObj(f);
    }

    @Override
    public TypedFunction<R> visit(BooleanFunction<T> f) {
        return obj.mapBoolean(f);
    }

    @Override
    public TypedFunction<R> visit(CharFunction<T> f) {
        return obj.mapChar(f);
    }

    @Override
    public TypedFunction<R> visit(ByteFunction<T> f) {
        return obj.mapByte(f);
    }

    @Override
    public TypedFunction<R> visit(ShortFunction<T> f) {
        return obj.mapShort(f);
    }

    @Override
    public TypedFunction<R> visit(io.deephaven.stream.blink.tf.IntFunction<T> f) {
        return obj.mapInt(f);
    }

    @Override
    public TypedFunction<R> visit(LongFunction<T> f) {
        return obj.mapLong(f);
    }

    @Override
    public TypedFunction<R> visit(FloatFunction<T> f) {
        return obj.mapFloat(f);
    }

    @Override
    public TypedFunction<R> visit(DoubleFunction<T> f) {
        return obj.mapDouble(f);
    }
}
