package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.Type;

import java.time.Instant;
import java.util.function.Function;
import java.util.function.ToLongFunction;

@FunctionalInterface
public interface LongFunction<T> extends PrimitiveFunction<T>, ToLongFunction<T> {
    /**
     * Assumes the object value is directly castable to a long. Equivalent to {@code x -> (long)x}.
     *
     * @return the long function
     * @param <T> the value type
     */
    static <T> LongFunction<T> primitive() {
        //noinspection unchecked
        return (LongFunction<T>) Functions.PrimitiveLong.INSTANCE;
    }

    static <T> LongFunction<T> cast(TypedFunction<T> f) {
        return (LongFunction<T>) f;
    }

    @Override
    long applyAsLong(T value);

    @Override
    default LongType returnType() {
        return Type.longType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    @Override
    default LongFunction<T> mapInput(Function<T, T> f) {
        return x -> applyAsLong(f.apply(x));
    }

    /**
     * Create a new function which returns {@code onNull} when the value is {@code null}, and otherwise calls {@link #applyAsLong(Object)}. Equivalent to {@code x -> x == null ? onNull : applyAsLong(x)}.
     * @param onNull the value to return on null
     * @return the new long function
     */
    default LongFunction<T> onNullInput(long onNull) {
        return x -> x == null ? onNull : applyAsLong(x);
    }

    default ObjectFunction<T, Instant> ofEpochMilli() {
        return mapObj(Instant::ofEpochMilli, Type.instantType());
    }

    default ObjectFunction<T, Instant> ofEpochSecond() {
        return mapObj(Instant::ofEpochSecond, Type.instantType());
    }

    @FunctionalInterface
    interface LongToObject<R> {
        R apply(long value);
    }

    default <R> ObjectFunction<T, R> mapObj(LongToObject<R> f, GenericType<R> returnType) {
        return ObjectFunction.of(t -> f.apply(applyAsLong(t)), returnType);
    }
}
