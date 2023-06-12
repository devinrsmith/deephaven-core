package io.deephaven.blink;

import io.deephaven.stream.blink.tf.BooleanFunction;
import io.deephaven.stream.blink.tf.ByteFunction;
import io.deephaven.stream.blink.tf.CharFunction;
import io.deephaven.stream.blink.tf.DoubleFunction;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.ShortFunction;
import io.deephaven.stream.blink.tf.TypedFunction.Visitor;

import java.util.Objects;
import java.util.function.Function;

public class ModifiedInputVisitor<T, R> implements Visitor<T, R> {
    private final Function<T, T> first;
    private final Visitor<T, R> delegate;

    public ModifiedInputVisitor(Function<T, T> first, Visitor<T, R> delegate) {
        this.first = Objects.requireNonNull(first);
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public R visit(BooleanFunction<T> f) {
        return delegate.visit((BooleanFunction<T>) value -> f.applyAsBoolean(first.apply(value)));
    }

    @Override
    public R visit(CharFunction<T> f) {
        return delegate.visit((CharFunction<T>) value -> f.applyAsChar(first.apply(value)));
    }

    @Override
    public R visit(ByteFunction<T> f) {
        return delegate.visit((ByteFunction<T>) value -> f.applyAsByte(first.apply(value)));
    }

    @Override
    public R visit(ShortFunction<T> f) {
        return delegate.visit((ShortFunction<T>) value -> f.applyAsShort(first.apply(value)));
    }

    @Override
    public R visit(IntFunction<T> f) {
        return delegate.visit((IntFunction<T>) value -> f.applyAsInt(first.apply(value)));
    }

    @Override
    public R visit(LongFunction<T> f) {
        return delegate.visit((LongFunction<T>) value -> f.applyAsLong(first.apply(value)));
    }

    @Override
    public R visit(FloatFunction<T> f) {
        return delegate.visit((FloatFunction<T>) value -> f.applyAsFloat(first.apply(value)));
    }

    @Override
    public R visit(DoubleFunction<T> f) {
        return delegate.visit((DoubleFunction<T>) value -> f.applyAsDouble(first.apply(value)));
    }

    @Override
    public R visit(ObjectFunction<T, ?> f) {
        return delegate.visit(adapt(f, first));
    }

    private static <T> ObjectFunction<T, ?> adapt(ObjectFunction<T, ?> f, Function<T, T> first) {
        //noinspection unchecked
        return adapt2((ObjectFunction<T, Object>) f, first);
    }

    private static <T> ObjectFunction<T, Object> adapt2(ObjectFunction<T, Object> f, Function<T, T> first) {
        return ObjectFunction.of(t -> f.apply(first.apply(t)), f.returnType());
    }
}
