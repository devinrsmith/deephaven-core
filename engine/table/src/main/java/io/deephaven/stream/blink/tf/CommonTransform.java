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
import io.deephaven.stream.blink.tf.PrimitiveFunction.Visitor;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.type.TypeUtils;

import java.time.Instant;
import java.util.Objects;

public class CommonTransform {

    /**
     * Potentially transform the {@link TypedFunction function} {@code f} into a common... todo
     *
     * @param f the function
     * @return the potentially transformed function
     * @param <T> the input type
     * @see #of(PrimitiveFunction)
     * @see #of(ObjectFunction)
     */
    public static <T> TypedFunction<T> of(TypedFunction<T> f) {
        return FunctionVisitor.of(f);
    }

    public static <T> TypedFunction<T> of(PrimitiveFunction<T> f) {
        // no transform currently; maybe boolean -> byte?
        return f;
        // return f.walk(new Visitor<>() {
        // @Override
        // public ObjectFunction<T, Boolean> visit(BooleanFunction<T> f) {
        // return ObjectFunction.of(f::applyAsBoolean, BoxedBooleanType.of());
        // }
        //
        // @Override
        // public TypedFunction<T> visit(CharFunction<T> f) {
        // return f;
        // }
        //
        // @Override
        // public TypedFunction<T> visit(ByteFunction<T> f) {
        // return f;
        // }
        //
        // @Override
        // public TypedFunction<T> visit(ShortFunction<T> f) {
        // return f;
        // }
        //
        // @Override
        // public TypedFunction<T> visit(IntFunction<T> f) {
        // return f;
        // }
        //
        // @Override
        // public TypedFunction<T> visit(LongFunction<T> f) {
        // return f;
        // }
        //
        // @Override
        // public TypedFunction<T> visit(FloatFunction<T> f) {
        // return f;
        // }
        //
        // @Override
        // public TypedFunction<T> visit(DoubleFunction<T> f) {
        // return f;
        // }
        // });
    }

    /**
     * Potentially transforms the {@link ObjectFunction Object function} {@code f} into a common... todo
     *
     * @param f the Object function
     * @return the potentially transformed function
     * @param <T> the input type
     * @see #toEpochNanos(ObjectFunction)
     * @see #unboxBooleanAsByte(ObjectFunction)
     * @see #unboxByte(ObjectFunction)
     * @see #unboxChar(ObjectFunction)
     * @see #unboxDouble(ObjectFunction)
     * @see #unboxFloat(ObjectFunction)
     * @see #unboxInt(ObjectFunction)
     * @see #unboxLong(ObjectFunction)
     * @see #unboxShort(ObjectFunction)
     */
    public static <T> TypedFunction<T> of(ObjectFunction<T, ?> f) {
        return ObjectFunctionVisitor.of(f);
    }

    /**
     * Equivalent to {@code f.mapByte(BooleanUtils::booleanAsByte)}.
     *
     * @param f the Boolean function
     * @return the byte function
     * @param <T> the input type
     * @see BooleanUtils#booleanAsByte(Boolean)
     */
    public static <T> ByteFunction<T> unboxBooleanAsByte(ObjectFunction<T, Boolean> f) {
        return f.mapByte(BooleanUtils::booleanAsByte);
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

    /**
     * Equivalent to {@code f.mapLong(DateTimeUtils::epochNanos)}.
     *
     * @param f the instant function
     * @return the epoch nanos function
     * @param <T> the input type
     * @see DateTimeUtils#epochNanos(Instant)
     */
    public static <T> LongFunction<T> toEpochNanos(ObjectFunction<T, Instant> f) {
        return f.mapLong(DateTimeUtils::epochNanos);
    }

    public static <T> ObjectFunction<T, ?> box(TypedFunction<T> f) {
        // noinspection unchecked
        return f.walk(
                (TypedFunction.Visitor<T, ObjectFunction<T, ?>>) (TypedFunction.Visitor<?, ?>) BoxedVisitor.INSTANCE);
    }

    public static <T, R> ObjectFunction<T, R> box(ObjectFunction<T, R> f) {
        return f;
    }

    public static <T> ObjectFunction<T, ?> box(PrimitiveFunction<T> f) {
        // noinspection unchecked
        return f.walk(
                (PrimitiveFunction.Visitor<T, ObjectFunction<T, ?>>) (PrimitiveFunction.Visitor<?, ?>) BoxedVisitor.INSTANCE);
    }

    public static <T> ObjectFunction<T, Boolean> box(BooleanFunction<T> f) {
        return f.mapObj(x -> x, BoxedBooleanType.of());
    }

    public static <T> ObjectFunction<T, Character> box(CharFunction<T> f) {
        return f.mapObj(TypeUtils::box, BoxedCharType.of());
    }

    public static <T> ObjectFunction<T, Byte> box(ByteFunction<T> f) {
        return f.mapObj(TypeUtils::box, BoxedByteType.of());
    }

    public static <T> ObjectFunction<T, Short> box(ShortFunction<T> f) {
        return f.mapObj(TypeUtils::box, BoxedShortType.of());
    }

    public static <T> ObjectFunction<T, Integer> box(IntFunction<T> f) {
        return f.mapObj(TypeUtils::box, BoxedIntType.of());
    }

    public static <T> ObjectFunction<T, Long> box(LongFunction<T> f) {
        return f.mapObj(TypeUtils::box, BoxedLongType.of());
    }

    public static <T> ObjectFunction<T, Float> box(FloatFunction<T> f) {
        return f.mapObj(TypeUtils::box, BoxedFloatType.of());
    }

    public static <T> ObjectFunction<T, Double> box(DoubleFunction<T> f) {
        return f.mapObj(TypeUtils::box, BoxedDoubleType.of());
    }

    private enum BoxedVisitor implements TypedFunction.Visitor<Object, ObjectFunction<Object, ?>>,
            PrimitiveFunction.Visitor<Object, ObjectFunction<Object, ?>> {
        INSTANCE;

        @Override
        public ObjectFunction<Object, ?> visit(PrimitiveFunction<Object> f) {
            return box(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(ObjectFunction<Object, ?> f) {
            return box(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(BooleanFunction<Object> f) {
            return box(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(CharFunction<Object> f) {
            return box(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(ByteFunction<Object> f) {
            return box(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(ShortFunction<Object> f) {
            return box(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(IntFunction<Object> f) {
            return box(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(LongFunction<Object> f) {
            return box(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(FloatFunction<Object> f) {
            return box(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(DoubleFunction<Object> f) {
            return box(f);
        }
    }

    private static class FunctionVisitor<T> implements TypedFunction.Visitor<T, TypedFunction<T>> {

        public static <T> TypedFunction<T> of(TypedFunction<T> f) {
            return f.walk(new FunctionVisitor<>());
        }

        private FunctionVisitor() {}

        @Override
        public TypedFunction<T> visit(PrimitiveFunction<T> f) {
            return CommonTransform.of(f);
        }

        @Override
        public TypedFunction<T> visit(ObjectFunction<T, ?> f) {
            return CommonTransform.of(f);
        }
    }

    private static class ObjectFunctionVisitor<T> implements
            GenericType.Visitor<TypedFunction<T>>,
            BoxedType.Visitor<TypedFunction<T>> {

        public static <T> TypedFunction<T> of(ObjectFunction<T, ?> f) {
            return f.returnType().walk(new ObjectFunctionVisitor<>(f));
        }

        private final ObjectFunction<T, ?> f;

        private ObjectFunctionVisitor(ObjectFunction<T, ?> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public TypedFunction<T> visit(BoxedType<?> boxedType) {
            return boxedType.walk((BoxedType.Visitor<TypedFunction<T>>) this);
        }

        @Override
        public TypedFunction<T> visit(InstantType instantType) {
            return toEpochNanos(f.as(instantType));
        }

        @Override
        public TypedFunction<T> visit(StringType stringType) {
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
            return unboxBooleanAsByte(f.as(booleanType));
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
