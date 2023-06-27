package io.deephaven.stream.blink.tf;

import io.deephaven.stream.blink.tf.TypedFunction.Visitor;

public class ApplyVisitor<T> implements Visitor<T, Object> {
    private final T value;

    public ApplyVisitor(T value) {
        this.value = value;
    }

    @Override
    public Object visit(BoxedBooleanFunction<T> f) {
        return f.applyAsBoolean(value);
    }

    @Override
    public Object visit(CharFunction<T> f) {
        return f.applyAsChar(value);
    }

    @Override
    public Object visit(ByteFunction<T> f) {
        return f.applyAsByte(value);
    }

    @Override
    public Object visit(ShortFunction<T> f) {
        return f.applyAsShort(value);
    }

    @Override
    public Object visit(IntFunction<T> f) {
        return f.applyAsInt(value);
    }

    @Override
    public Object visit(LongFunction<T> f) {
        return f.applyAsLong(value);
    }

    @Override
    public Object visit(FloatFunction<T> f) {
        return f.applyAsFloat(value);
    }

    @Override
    public Object visit(DoubleFunction<T> f) {
        return f.applyAsDouble(value);
    }

    @Override
    public Object visit(ObjectFunction<T, ?> f) {
        return f.apply(value);
    }
}
