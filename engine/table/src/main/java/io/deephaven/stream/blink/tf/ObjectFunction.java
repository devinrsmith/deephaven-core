package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.GenericType;

import java.util.function.Function;

public interface ObjectFunction<T, R> extends TypedFunction<T> {

    static <T, R> ObjectFunction<T, R> of(Function<T, R> f, GenericType<R> returnType) {
        return new ObjectFunctionImpl<>(f, returnType);
    }

    GenericType<R> returnType();

    R apply(T value);

    @Override
    default <Z> Z walk(Visitor<T, Z> visitor) {
        return visitor.visit(this);
    }

    default ObjectFunction<T, R> onNull(R onNull) {
        return ObjectFunction.of(x -> x == null ? onNull : apply(x), returnType());
    }

    @Override
    default ObjectFunction<T, R> mapInput(Function<T, T> f) {
        return ObjectFunction.of(x -> apply(f.apply(x)), returnType());
    }

    default <R2> ObjectFunction<T, R2> map(ObjectFunction<R, R2> f) {
        return ObjectFunction.of(t -> f.apply(ObjectFunction.this.apply(t)), f.returnType());
    }

    default <R2> ObjectFunction<T, R2> map(Function<R, R2> f, GenericType<R2> returnType) {
        return ObjectFunction.of(t -> f.apply(ObjectFunction.this.apply(t)), returnType);
    }

    default IntFunction<T> mapToInt(IntFunction<R> f) {
        return value -> f.applyAsInt(apply(value));
    }

    default LongFunction<T> mapToLong(LongFunction<R> f) {
        return value -> f.applyAsLong(apply(value));
    }

    default FloatFunction<T> mapToFloat(FloatFunction<R> f) {
        return value -> f.applyAsFloat(apply(value));
    }

    default DoubleFunction<T> mapToDouble(DoubleFunction<R> f) {
        return value -> f.applyAsDouble(apply(value));
    }
}
