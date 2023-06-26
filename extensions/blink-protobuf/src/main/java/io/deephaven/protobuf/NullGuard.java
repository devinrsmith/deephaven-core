package io.deephaven.protobuf;

import io.deephaven.stream.blink.tf.BooleanFunction;
import io.deephaven.stream.blink.tf.ByteFunction;
import io.deephaven.stream.blink.tf.CharFunction;
import io.deephaven.stream.blink.tf.DoubleFunction;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.ShortFunction;
import io.deephaven.stream.blink.tf.TypedFunction;
import io.deephaven.stream.blink.tf.TypedFunction.Visitor;
import io.deephaven.util.QueryConstants;

enum NullGuard implements TypedFunction.Visitor<Object, TypedFunction<?>> {
    INSTANCE;

    public static <T> TypedFunction<T> of(TypedFunction<T> f) {
        // noinspection unchecked
        return (TypedFunction<T>) f.walk((Visitor<T, ?>) INSTANCE);
    }

    public static <T> BooleanFunction<T> of(BooleanFunction<T> f) {
        return f.onNullInput(null);
    }

    public static <T> CharFunction<T> of(CharFunction<T> f) {
        return f.onNullInput(QueryConstants.NULL_CHAR);
    }

    public static <T> ByteFunction<T> of(ByteFunction<T> f) {
        return f.onNullInput(QueryConstants.NULL_BYTE);
    }

    public static <T> ShortFunction<T> of(ShortFunction<T> f) {
        return f.onNullInput(QueryConstants.NULL_SHORT);
    }

    public static <T> IntFunction<T> of(IntFunction<T> f) {
        return f.onNullInput(QueryConstants.NULL_INT);
    }

    public static <T> LongFunction<T> of(LongFunction<T> f) {
        return f.onNullInput(QueryConstants.NULL_LONG);
    }

    public static <T> FloatFunction<T> of(FloatFunction<T> f) {
        return f.onNullInput(QueryConstants.NULL_FLOAT);
    }

    public static <T> DoubleFunction<T> of(DoubleFunction<T> f) {
        return f.onNullInput(QueryConstants.NULL_DOUBLE);
    }

    public static <T, R> ObjectFunction<T, R> of(ObjectFunction<T, R> f) {
        return f.onNullInput(null);
    }

    @Override
    public TypedFunction<?> visit(BooleanFunction<Object> f) {
        return of(f);
    }

    @Override
    public TypedFunction<?> visit(CharFunction<Object> f) {
        return of(f);
    }

    @Override
    public TypedFunction<?> visit(ByteFunction<Object> f) {
        return of(f);
    }

    @Override
    public TypedFunction<?> visit(ShortFunction<Object> f) {
        return of(f);
    }

    @Override
    public TypedFunction<?> visit(IntFunction<Object> f) {
        return of(f);
    }

    @Override
    public TypedFunction<?> visit(LongFunction<Object> f) {
        return of(f);
    }

    @Override
    public TypedFunction<?> visit(FloatFunction<Object> f) {
        return of(f);
    }

    @Override
    public TypedFunction<?> visit(DoubleFunction<Object> f) {
        return of(f);
    }

    @Override
    public TypedFunction<?> visit(ObjectFunction<Object, ?> f) {
        return of(f);
    }
}
