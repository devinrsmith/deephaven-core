package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.GenericType;

import java.util.function.Function;

public interface ObjectFunction<T, R> extends TypedFunction<T> {

    static <T, R> ObjectFunction<T, R> of(Function<T, R> f, GenericType<R> returnType) {
        return new ObjectFunctionImpl<>(f, returnType);
    }

    static <T, R> ObjectFunction<T, R> cast(GenericType<R> expected) {
        return ObjectFunction.of(t -> expected.clazz().cast(t), expected);
    }

    static <T, R> ObjectFunction<T, R> ofSingle(R sentinel, GenericType<R> type) {
        return new ObjectFunctionImpl<>(m -> sentinel, type);
    }

    static <R> ObjectFunction<R, R> identity(GenericType<R> type) {
        return ObjectFunction.of(Function.identity(), type);
    }

    GenericType<R> returnType();

    R apply(T value);

    @Override
    default <Z> Z walk(Visitor<T, Z> visitor) {
        return visitor.visit(this);
    }

    default ObjectFunction<T, R> onNullInput(R onNull) {
        return ObjectFunction.of(x -> x == null ? onNull : apply(x), returnType());
    }

    @Override
    default ObjectFunction<T, R> mapInput(Function<T, T> f) {
        return ObjectFunction.of(x -> apply(f.apply(x)), returnType());
    }

    default <R2> ObjectFunction<T, R2> as(GenericType<R2> type) {
        return map(cast(type));
    }


    default <R2> ObjectFunction<T, R2> map(Function<R, R2> f, GenericType<R2> returnType) {
        return ObjectFunction.of(t -> f.apply(ObjectFunction.this.apply(t)), returnType);
    }

    default BooleanFunction<T> map(BooleanFunction<R> f) {
        return value -> f.applyAsBoolean(apply(value));
    }

    default CharFunction<T> map(CharFunction<R> f) {
        return value -> f.applyAsChar(apply(value));
    }

    default ByteFunction<T> map(ByteFunction<R> f) {
        return value -> f.applyAsByte(apply(value));
    }

    default ShortFunction<T> map(ShortFunction<R> f) {
        return value -> f.applyAsShort(apply(value));
    }

    default IntFunction<T> map(IntFunction<R> f) {
        return value -> f.applyAsInt(apply(value));
    }

    default LongFunction<T> map(LongFunction<R> f) {
        return value -> f.applyAsLong(apply(value));
    }

    default FloatFunction<T> map(FloatFunction<R> f) {
        return value -> f.applyAsFloat(apply(value));
    }

    default DoubleFunction<T> map(DoubleFunction<R> f) {
        return value -> f.applyAsDouble(apply(value));
    }

    default <R2> ObjectFunction<T, R2> map(ObjectFunction<R, R2> f) {
        return ObjectFunction.of(t -> f.apply(ObjectFunction.this.apply(t)), f.returnType());
    }

    default TypedFunction<T> map(TypedFunction<R> f) {
        return f.walk(new Visitor<>() {
            @Override
            public TypedFunction<T> visit(BooleanFunction<R> f) {
                return map(f);
            }

            @Override
            public TypedFunction<T> visit(CharFunction<R> f) {
                return map(f);
            }

            @Override
            public TypedFunction<T> visit(ByteFunction<R> f) {
                return map(f);
            }

            @Override
            public TypedFunction<T> visit(ShortFunction<R> f) {
                return map(f);
            }

            @Override
            public TypedFunction<T> visit(IntFunction<R> f) {
                return map(f);
            }

            @Override
            public TypedFunction<T> visit(LongFunction<R> f) {
                return map(f);
            }

            @Override
            public TypedFunction<T> visit(FloatFunction<R> f) {
                return map(f);
            }

            @Override
            public TypedFunction<T> visit(DoubleFunction<R> f) {
                return map(f);
            }

            @Override
            public TypedFunction<T> visit(ObjectFunction<R, ?> f) {
                return map(f);
            }
        });
    }
}
