package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
import io.deephaven.util.type.TypeUtils;

public class BoxTransform {
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
        return f.mapObj(x -> x, BoxedBooleanType.of());
    }

    public static <T> ObjectFunction<T, Character> of(CharFunction<T> f) {
        return f.mapObj(TypeUtils::box, BoxedCharType.of());
    }

    public static <T> ObjectFunction<T, Byte> of(ByteFunction<T> f) {
        return f.mapObj(TypeUtils::box, BoxedByteType.of());
    }

    public static <T> ObjectFunction<T, Short> of(ShortFunction<T> f) {
        return f.mapObj(TypeUtils::box, BoxedShortType.of());
    }

    public static <T> ObjectFunction<T, Integer> of(IntFunction<T> f) {
        return f.mapObj(TypeUtils::box, BoxedIntType.of());
    }

    public static <T> ObjectFunction<T, Long> of(LongFunction<T> f) {
        return f.mapObj(TypeUtils::box, BoxedLongType.of());
    }

    public static <T> ObjectFunction<T, Float> of(FloatFunction<T> f) {
        return f.mapObj(TypeUtils::box, BoxedFloatType.of());
    }

    public static <T> ObjectFunction<T, Double> of(DoubleFunction<T> f) {
        return f.mapObj(TypeUtils::box, BoxedDoubleType.of());
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
