package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.CharFunction.CharToObject;

import java.util.function.Function;

@FunctionalInterface
public interface BooleanFunction<T> extends PrimitiveFunction<T> {

    /**
     * Assumes the object value is directly castable to a boolean. Equivalent to {@code x -> (boolean)x}.
     *
     * @return the boolean function
     * @param <T> the value type
     */
    static <T> BooleanFunction<T> primitive() {
        //noinspection unchecked
        return (BooleanFunction<T>) Functions.PrimitiveBoolean.INSTANCE;
    }

    static <T> BooleanFunction<T> cast(TypedFunction<T> f) {
        return (BooleanFunction<T>) f;
    }

    boolean applyAsBoolean(T value);

    @Override
    default BooleanType returnType() {
        return Type.booleanType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    @Override
    default BooleanFunction<T> mapInput(Function<T, T> f) {
        return x -> applyAsBoolean(f.apply(x));
    }

    default BooleanFunction<T> onNullInput(boolean onNull) {
        return x -> x == null ? onNull : applyAsBoolean(x);
    }

    @FunctionalInterface
    interface BoolToObject<R> {
        R apply(boolean value);
    }

    default <R> ObjectFunction<T, R> mapObj(BoolToObject<R> f, GenericType<R> returnType) {
        return ObjectFunction.of(t -> f.apply(applyAsBoolean(t)), returnType);
    }
}
