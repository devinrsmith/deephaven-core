package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
import io.deephaven.stream.blink.tf.BooleanFunction.BoolToObject;
import io.deephaven.stream.blink.tf.ByteFunction.ByteToObject;
import io.deephaven.stream.blink.tf.CharFunction.CharToObject;
import io.deephaven.stream.blink.tf.DoubleFunction.DoubleToObject;
import io.deephaven.stream.blink.tf.FloatFunction.FloatToObject;
import io.deephaven.stream.blink.tf.IntFunction.IntToObject;
import io.deephaven.stream.blink.tf.LongFunction.LongToObject;
import io.deephaven.stream.blink.tf.ShortFunction.ShortToObject;
import io.deephaven.util.type.TypeUtils;

public class BoxTransform {

    public static final BoolToObject<Boolean> BOX_BOOL = x -> x;
    private static final CharToObject<Character> BOX_CHAR = TypeUtils::box;
    private static final ByteToObject<Byte> BOX_BYTE = TypeUtils::box;
    private static final ShortToObject<Short> BOX_SHORT = TypeUtils::box;
    private static final IntToObject<Integer> BOX_INT = TypeUtils::box;
    private static final LongToObject<Long> BOX_LONG = TypeUtils::box;
    private static final FloatToObject<Float> BOX_FLOAT = TypeUtils::box;
    private static final DoubleToObject<Double> BOX_DOUBLE = TypeUtils::box;

    public static <T> ObjectFunction<T, ?> of(TypedFunction<T> f) {
        return BoxedVisitor.of(f);
    }

    public static <T, R> ObjectFunction<T, R> of(ObjectFunction<T, R> f) {
        return f;
    }

    public static <T> ObjectFunction<T, ?> of(PrimitiveFunction<T> f) {
        return BoxedVisitor.of(f);
    }

    public static <T> ObjectFunction<T, Boolean> of(BooleanFunction<T> f) {
        return f.mapObj(BOX_BOOL, BoxedBooleanType.of());
    }

    public static <T> ObjectFunction<T, Character> of(CharFunction<T> f) {
        return f.mapObj(BOX_CHAR, BoxedCharType.of());
    }

    public static <T> ObjectFunction<T, Byte> of(ByteFunction<T> f) {
        return f.mapObj(BOX_BYTE, BoxedByteType.of());
    }

    public static <T> ObjectFunction<T, Short> of(ShortFunction<T> f) {
        return f.mapObj(BOX_SHORT, BoxedShortType.of());
    }

    public static <T> ObjectFunction<T, Integer> of(IntFunction<T> f) {
        return f.mapObj(BOX_INT, BoxedIntType.of());
    }

    public static <T> ObjectFunction<T, Long> of(LongFunction<T> f) {
        return f.mapObj(BOX_LONG, BoxedLongType.of());
    }

    public static <T> ObjectFunction<T, Float> of(FloatFunction<T> f) {
        return f.mapObj(BOX_FLOAT, BoxedFloatType.of());
    }

    public static <T> ObjectFunction<T, Double> of(DoubleFunction<T> f) {
        return f.mapObj(BOX_DOUBLE, BoxedDoubleType.of());
    }

    private enum BoxedVisitor implements TypedFunction.Visitor<Object, ObjectFunction<Object, ?>>,
            PrimitiveFunction.Visitor<Object, ObjectFunction<Object, ?>> {
        INSTANCE;

        public static <T> ObjectFunction<T, ?> of(TypedFunction<T> f) {
            // noinspection unchecked
            return f.walk((TypedFunction.Visitor<T, ObjectFunction<T, ?>>) (TypedFunction.Visitor<?, ?>) INSTANCE);
        }

        public static <T> ObjectFunction<T, ?> of(PrimitiveFunction<T> f) {
            // noinspection unchecked
            return f.walk(
                    (PrimitiveFunction.Visitor<T, ObjectFunction<T, ?>>) (PrimitiveFunction.Visitor<?, ?>) INSTANCE);
        }

        @Override
        public ObjectFunction<Object, ?> visit(PrimitiveFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(ObjectFunction<Object, ?> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(BooleanFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(CharFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(ByteFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(ShortFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(IntFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(LongFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(FloatFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(DoubleFunction<Object> f) {
            return BoxTransform.of(f);
        }
    }
}
