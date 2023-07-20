package io.deephaven.protobuf;

import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
import io.deephaven.stream.blink.tf.BooleanFunction;
import io.deephaven.stream.blink.tf.ByteFunction;
import io.deephaven.stream.blink.tf.CharFunction;
import io.deephaven.stream.blink.tf.DoubleFunction;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.PrimitiveFunction;
import io.deephaven.stream.blink.tf.ShortFunction;
import io.deephaven.stream.blink.tf.TypedFunction;

enum BypassOnNull implements
        TypedFunction.Visitor<Object, ObjectFunction<Object, ?>>,
        PrimitiveFunction.Visitor<Object, ObjectFunction<Object, ?>> {
    INSTANCE;

    public static <T> TypedFunction.Visitor<T, ObjectFunction<T, ?>> visitor() {
        // noinspection unchecked,rawtypes
        return (TypedFunction.Visitor<T, ObjectFunction<T, ?>>) (TypedFunction.Visitor) INSTANCE;
    }

    public static <T> ObjectFunction<T, ?> of(TypedFunction<T> x) {
        return x.walk(visitor());
    }

    @Override
    public ObjectFunction<Object, ?> visit(ObjectFunction<Object, ?> f) {
        return f.onNullInput(null);
    }

    @Override
    public ObjectFunction<Object, ?> visit(PrimitiveFunction<Object> f) {
        return f.walk((PrimitiveFunction.Visitor<Object, ObjectFunction<Object, ?>>) this);
    }

    @Override
    public ObjectFunction<Object, Boolean> visit(BooleanFunction<Object> f) {
        return ObjectFunction.of(t -> t == null ? null : f.applyAsBoolean(t), BoxedBooleanType.of());
    }

    @Override
    public ObjectFunction<Object, Character> visit(CharFunction<Object> f) {
        return ObjectFunction.of(t -> t == null ? null : f.applyAsChar(t), BoxedCharType.of());
    }

    @Override
    public ObjectFunction<Object, Byte> visit(ByteFunction<Object> f) {
        return ObjectFunction.of(t -> t == null ? null : f.applyAsByte(t), BoxedByteType.of());
    }

    @Override
    public ObjectFunction<Object, Short> visit(ShortFunction<Object> f) {
        return ObjectFunction.of(t -> t == null ? null : f.applyAsShort(t), BoxedShortType.of());
    }

    @Override
    public ObjectFunction<Object, Integer> visit(IntFunction<Object> f) {
        return ObjectFunction.of(t -> t == null ? null : f.applyAsInt(t), BoxedIntType.of());
    }

    @Override
    public ObjectFunction<Object, Long> visit(LongFunction<Object> f) {
        return ObjectFunction.of(t -> t == null ? null : f.applyAsLong(t), BoxedLongType.of());
    }

    @Override
    public ObjectFunction<Object, Float> visit(FloatFunction<Object> f) {
        return ObjectFunction.of(t -> t == null ? null : f.applyAsFloat(t), BoxedFloatType.of());
    }

    @Override
    public ObjectFunction<Object, Double> visit(DoubleFunction<Object> f) {
        return ObjectFunction.of(t -> t == null ? null : f.applyAsDouble(t), BoxedDoubleType.of());
    }
}
