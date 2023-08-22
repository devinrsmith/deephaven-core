package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

/**
 * A {@code byte} function.
 *
 * @param <T> the input type
 */
@FunctionalInterface
public interface ByteFunction<T> extends PrimitiveFunction<T> {

    /**
     * Assumes the object value is directly castable to a byte. Equivalent to {@code x -> (byte)x}.
     *
     * @return the byte function
     * @param <T> the value type
     */
    static <T> ByteFunction<T> primitive() {
        //noinspection unchecked
        return (ByteFunction<T>) Functions.PrimitiveByte.INSTANCE;
    }

    static <T> ByteFunction<T> cast(TypedFunction<T> f) {
        return (ByteFunction<T>) f;
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
    static <T, R> ByteFunction<T> map(Function<T, R> f, ByteFunction<R> g) {
        return new ByteMap<>(f, g);
    }

    byte applyAsByte(T value);

    @Override
    default ByteType returnType() {
        return Type.byteType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    @Override
    default ByteFunction<T> mapInput(Function<T, T> f) {
        return x -> applyAsByte(f.apply(x));
    }

    @FunctionalInterface
    interface ByteToObject<R> {
        R apply(byte value);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.apply(this.applyAsByte(x))}.
     *
     * @param g the outer function
     * @return the object function
     */
    default <R> ObjectFunction<T, R> mapObj(ByteToObject<R> g, GenericType<R> returnType) {
        return new ByteToObjectMap<>(this, g, returnType);
    }
}
