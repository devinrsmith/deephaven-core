package io.deephaven.stream.blink.tf;

public class DhNullableTypeTransform {

    public static <X> TypedFunction<X> of(TypedFunction<X> f) {
        return NullableTypeVisitor.of(f);
    }

    public static <X> TypedFunction<X> of(PrimitiveFunction<X> f) {
        return NullableTypeVisitor.of(f);
    }

    private enum NullableTypeVisitor implements TypedFunction.Visitor<Object, TypedFunction<Object>>,
            PrimitiveFunction.Visitor<Object, TypedFunction<Object>> {
        INSTANCE;

        public static <X> TypedFunction<X> of(TypedFunction<X> f) {
            //noinspection unchecked
            return f.walk((TypedFunction.Visitor<X, TypedFunction<X>>) (TypedFunction.Visitor<?, ?>) INSTANCE);
        }

        public static <X> TypedFunction<X> of(PrimitiveFunction<X> f) {
            //noinspection unchecked
            return f.walk((PrimitiveFunction.Visitor<X, TypedFunction<X>>) (PrimitiveFunction.Visitor<?, ?>) INSTANCE);
        }

        @Override
        public TypedFunction<Object> visit(PrimitiveFunction<Object> f) {
            return of(f);
        }

        @Override
        public TypedFunction<Object> visit(ObjectFunction<Object, ?> f) {
            return f;
        }

        @Override
        public ObjectFunction<Object, Boolean> visit(BooleanFunction<Object> f) {
            // BooleanFunction is the only function / primitive type that doesn't natively have a "null" type.
            return BoxTransform.of(f);
        }

        @Override
        public TypedFunction<Object> visit(CharFunction<Object> f) {
            return f;
        }

        @Override
        public TypedFunction<Object> visit(ByteFunction<Object> f) {
            return f;
        }

        @Override
        public TypedFunction<Object> visit(ShortFunction<Object> f) {
            return f;
        }

        @Override
        public TypedFunction<Object> visit(IntFunction<Object> f) {
            return f;
        }

        @Override
        public TypedFunction<Object> visit(LongFunction<Object> f) {
            return f;
        }

        @Override
        public TypedFunction<Object> visit(FloatFunction<Object> f) {
            return f;
        }

        @Override
        public TypedFunction<Object> visit(DoubleFunction<Object> f) {
            return f;
        }
    }
}
