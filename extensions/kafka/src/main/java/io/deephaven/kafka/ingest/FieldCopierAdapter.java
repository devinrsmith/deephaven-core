package io.deephaven.kafka.ingest;

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

public enum FieldCopierAdapter
        implements TypedFunction.Visitor<Object, FieldCopier>, PrimitiveFunction.Visitor<Object, FieldCopier> {
    INSTANCE;

    public static FieldCopier of(TypedFunction<Object> f) {
        return f.walk(INSTANCE);
    }

    public static FieldCopier of(PrimitiveFunction<Object> f) {
        return f.walk((PrimitiveFunction.Visitor<Object, FieldCopier>) INSTANCE);
    }

    public static FieldCopier of(BooleanFunction<Object> f) {
        return BooleanFieldCopier.of(f);
    }

    public static FieldCopier of(CharFunction<Object> f) {
        return CharFieldCopier.of(f);
    }

    public static FieldCopier of(ByteFunction<Object> f) {
        return ByteFieldCopier.of(f);
    }

    public static FieldCopier of(ShortFunction<Object> f) {
        return ShortFieldCopier.of(f);
    }

    public static FieldCopier of(IntFunction<Object> f) {
        return IntFieldCopier.of(f);
    }

    public static FieldCopier of(LongFunction<Object> f) {
        return LongFieldCopier.of(f);
    }

    public static FieldCopier of(FloatFunction<Object> f) {
        return FloatFieldCopier.of(f);
    }

    public static FieldCopier of(DoubleFunction<Object> f) {
        return DoubleFieldCopier.of(f);
    }

    public static FieldCopier of(ObjectFunction<Object, ?> f) {
        return ObjectFieldCopier.of(f);
    }

    @Override
    public FieldCopier visit(PrimitiveFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(BooleanFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(CharFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(ByteFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(ShortFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(IntFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(LongFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(FloatFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(DoubleFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(ObjectFunction<Object, ?> f) {
        return of(f);
    }
}
