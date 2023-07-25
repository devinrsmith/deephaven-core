package io.deephaven.kafka.ingest;

import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType.Visitor;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.StringType;
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
import io.deephaven.time.DateTimeUtils;

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

    public static IntFieldCopier of(IntFunction<Object> f) {
        return IntFieldCopier.of(f);
    }

    public static LongFieldCopier of(LongFunction<Object> f) {
        return LongFieldCopier.of(f);
    }

    public static FloatFieldCopier of(FloatFunction<Object> f) {
        return FloatFieldCopier.of(f);
    }

    public static DoubleFieldCopier of(DoubleFunction<Object> f) {
        return DoubleFieldCopier.of(f);
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
        return f.returnType().walk(new Visitor<>() {
            @Override
            public FieldCopier visit(BoxedType<?> boxedType) {
                return boxedType.walk(new BoxedType.Visitor<>() {
                    @Override
                    public FieldCopier visit(BoxedBooleanType booleanType) {
                        return ByteFieldCopier.ofBoolean(f.as(booleanType));
                    }

                    @Override
                    public FieldCopier visit(BoxedByteType byteType) {
                        return ByteFieldCopier.of(f.as(byteType));
                    }

                    @Override
                    public FieldCopier visit(BoxedCharType charType) {
                        return CharFieldCopier.of(f.as(charType));
                    }

                    @Override
                    public FieldCopier visit(BoxedShortType shortType) {
                        return ShortFieldCopier.of(f.as(shortType));
                    }

                    @Override
                    public FieldCopier visit(BoxedIntType intType) {
                        return IntFieldCopier.of(f.as(intType));
                    }

                    @Override
                    public FieldCopier visit(BoxedLongType longType) {
                        return LongFieldCopier.of(f.as(longType));
                    }

                    @Override
                    public FieldCopier visit(BoxedFloatType floatType) {
                        return FloatFieldCopier.of(f.as(floatType));
                    }

                    @Override
                    public FieldCopier visit(BoxedDoubleType doubleType) {
                        return DoubleFieldCopier.of(f.as(doubleType));
                    }
                });
            }

            @Override
            public FieldCopier visit(StringType stringType) {
                return ObjectFieldCopier.of(f.as(stringType));
            }

            @Override
            public FieldCopier visit(InstantType instantType) {
                return LongFieldCopier.of(f.as(instantType).mapLong(DateTimeUtils::epochNanos));
            }

            @Override
            public FieldCopier visit(ArrayType<?, ?> arrayType) {
                return ObjectFieldCopier.of(f.as(arrayType));
            }

            @Override
            public FieldCopier visit(CustomType<?> customType) {
                return ObjectFieldCopier.of(f.as(customType));
            }
        });
    }
}
