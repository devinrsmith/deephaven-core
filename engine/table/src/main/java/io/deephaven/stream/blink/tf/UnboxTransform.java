package io.deephaven.stream.blink.tf;

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
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.StringType;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

public class UnboxTransform {

    /**
     * Returns the Deephaven unboxed equivalent of {@code f}. Relevant for all {@link BoxedType boxed types} except the
     * {@link BoxedBooleanType boxed Boolean type}. All other functions will be return unchanged.
     *
     * @param f the function
     * @return the unboxed equivalent
     * @param <T> the input type
     * @see #unboxByte(ObjectFunction)
     * @see #unboxChar(ObjectFunction)
     * @see #unboxDouble(ObjectFunction)
     * @see #unboxFloat(ObjectFunction)
     * @see #unboxInt(ObjectFunction)
     * @see #unboxLong(ObjectFunction)
     * @see #unboxShort(ObjectFunction)
     */
    public static <T> TypedFunction<T> of(TypedFunction<T> f) {
        return UnboxFunctionVisitor.of(f);
    }

    /**
     * Returns the Deephaven unboxed equivalent of {@code f}. Relevant for all {@link BoxedType boxed types} except the
     * {@link BoxedBooleanType boxed Boolean type}. All other functions will be return unchanged.
     *
     * @param f the object function
     * @return the unboxed equivalent
     * @param <T> the input type
     * @see #unboxByte(ObjectFunction)
     * @see #unboxChar(ObjectFunction)
     * @see #unboxDouble(ObjectFunction)
     * @see #unboxFloat(ObjectFunction)
     * @see #unboxInt(ObjectFunction)
     * @see #unboxLong(ObjectFunction)
     * @see #unboxShort(ObjectFunction)
     */
    public static <T> TypedFunction<T> of(ObjectFunction<T, ?> f) {
        return UnboxObjectFunctionVisitor.of(f);
    }

    /**
     * Equivalent to {@code f.mapByte(TypeUtils::unbox)}.
     *
     * @param f the Byte function
     * @return the byte function
     * @param <T> the input type
     * @see TypeUtils#unbox(Byte)
     */
    public static <T> ByteFunction<T> unboxByte(ObjectFunction<T, Byte> f) {
        return f.mapByte(TypeUtils::unbox);
    }

    /**
     * Equivalent to {@code f.mapChar(TypeUtils::unbox)}.
     *
     * @param f the Character function
     * @return the char function
     * @param <T> the input type
     * @see TypeUtils#unbox(Character)
     */
    public static <T> CharFunction<T> unboxChar(ObjectFunction<T, Character> f) {
        return f.mapChar(TypeUtils::unbox);
    }

    /**
     * Equivalent to {@code f.mapShort(TypeUtils::unbox)}.
     *
     * @param f the Short function
     * @return the short function
     * @param <T> the input type
     * @see TypeUtils#unbox(Short)
     */
    public static <T> ShortFunction<T> unboxShort(ObjectFunction<T, Short> f) {
        return f.mapShort(TypeUtils::unbox);
    }

    /**
     * Equivalent to {@code f.mapInt(TypeUtils::unbox)}.
     *
     * @param f the Integer function
     * @return the int function
     * @param <T> the input type
     * @see TypeUtils#unbox(Integer)
     */
    public static <T> IntFunction<T> unboxInt(ObjectFunction<T, Integer> f) {
        return f.mapInt(TypeUtils::unbox);
    }

    /**
     * Equivalent to {@code f.mapLong(TypeUtils::unbox)}.
     *
     * @param f the Long function
     * @return the long function
     * @param <T> the input type
     * @see TypeUtils#unbox(Long)
     */
    public static <T> LongFunction<T> unboxLong(ObjectFunction<T, Long> f) {
        return f.mapLong(TypeUtils::unbox);
    }

    /**
     * Equivalent to {@code f.mapFloat(TypeUtils::unbox)}.
     *
     * @param f the Float function
     * @return the float function
     * @param <T> the input type
     * @see TypeUtils#unbox(Float)
     */
    public static <T> FloatFunction<T> unboxFloat(ObjectFunction<T, Float> f) {
        return f.mapFloat(TypeUtils::unbox);
    }

    /**
     * Equivalent to {@code f.mapDouble(TypeUtils::unbox)}.
     *
     * @param f the Double function
     * @return the double function
     * @param <T> the input type
     * @see TypeUtils#unbox(Double)
     */
    public static <T> DoubleFunction<T> unboxDouble(ObjectFunction<T, Double> f) {
        return f.mapDouble(TypeUtils::unbox);
    }

    private enum UnboxFunctionVisitor implements TypedFunction.Visitor<Object, TypedFunction<Object>> {
        INSTANCE;

        public static <T> TypedFunction<T> of(TypedFunction<T> f) {
            // noinspection unchecked
            return f.walk((TypedFunction.Visitor<T, TypedFunction<T>>) (TypedFunction.Visitor<?, ?>) INSTANCE);
        }

        @Override
        public TypedFunction<Object> visit(PrimitiveFunction<Object> f) {
            return f;
        }

        @Override
        public TypedFunction<Object> visit(ObjectFunction<Object, ?> f) {
            return UnboxTransform.of(f);
        }
    }

    private static class UnboxObjectFunctionVisitor<T>
            implements GenericType.Visitor<TypedFunction<T>>, BoxedType.Visitor<TypedFunction<T>> {

        public static <T> TypedFunction<T> of(ObjectFunction<T, ?> f) {
            return f.returnType().walk(new UnboxObjectFunctionVisitor<>(f));
        }

        private final ObjectFunction<T, ?> f;

        public UnboxObjectFunctionVisitor(ObjectFunction<T, ?> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public TypedFunction<T> visit(BoxedType<?> boxedType) {
            return boxedType.walk((BoxedType.Visitor<TypedFunction<T>>) this);
        }

        @Override
        public TypedFunction<T> visit(StringType stringType) {
            return f;
        }

        @Override
        public TypedFunction<T> visit(InstantType instantType) {
            return f;
        }

        @Override
        public TypedFunction<T> visit(ArrayType<?, ?> arrayType) {
            return f;
        }

        @Override
        public TypedFunction<T> visit(CustomType<?> customType) {
            return f;
        }

        @Override
        public TypedFunction<T> visit(BoxedBooleanType booleanType) {
            // We don't have an "unboxed boolean".
            // We _can_ transform it to a byte, but that's a separate operation.
            return f;
        }

        @Override
        public TypedFunction<T> visit(BoxedByteType byteType) {
            return unboxByte(f.as(byteType));
        }

        @Override
        public TypedFunction<T> visit(BoxedCharType charType) {
            return unboxChar(f.as(charType));
        }

        @Override
        public TypedFunction<T> visit(BoxedShortType shortType) {
            return unboxShort(f.as(shortType));
        }

        @Override
        public TypedFunction<T> visit(BoxedIntType intType) {
            return unboxInt(f.as(intType));
        }

        @Override
        public TypedFunction<T> visit(BoxedLongType longType) {
            return unboxLong(f.as(longType));
        }

        @Override
        public TypedFunction<T> visit(BoxedFloatType floatType) {
            return unboxFloat(f.as(floatType));
        }

        @Override
        public TypedFunction<T> visit(BoxedDoubleType doubleType) {
            return unboxDouble(f.as(doubleType));
        }
    }
}
