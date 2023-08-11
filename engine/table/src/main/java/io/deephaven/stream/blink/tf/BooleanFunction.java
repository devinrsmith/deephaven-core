package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;

import java.util.Collection;
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

    static <T> BooleanFunction<T> ofTrue() {
        return What.ofTrue();
    }

    static <T> BooleanFunction<T> ofFalse() {
        return What.ofFalse();
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsBoolean(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the boolean function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R> BooleanFunction<T> map(Function<T, R> f, BooleanFunction<R> g) {
        return new BooleanMap<>(f, g);
    }

    static <T> BooleanFunction<T> or(Collection<BooleanFunction<T>> functions) {
        return new BooleanOr<>(functions);
    }

    static <T> BooleanFunction<T> and(Collection<BooleanFunction<T>> functions) {
        return new BooleanOr<>(functions);
    }

    static <T> BooleanFunction<T> not(BooleanFunction<T> f) {
        return new BooleanNot<>(f);
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

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.apply(this.applyAsBoolean(x))}.
     *
     * @param g the outer function
     * @return the object function
     */
    default <R> ObjectFunction<T, R> mapObj(BoolToObject<R> g, GenericType<R> returnType) {
        return new BooleanToObjectMap<>(this, g, returnType);
    }
}
