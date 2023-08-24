package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

/**
 * A {@code char} function.
 *
 * @param <T> the input type
 */
@FunctionalInterface
public interface CharFunction<T> extends PrimitiveFunction<T> {
    /**
     * Assumes the object value is directly castable to a char. Equivalent to {@code x -> (char)x}.
     *
     * @return the char function
     * @param <T> the value type
     */
    static <T> CharFunction<T> primitive() {
        //noinspection unchecked
        return (CharFunction<T>) Functions.PrimitiveChar.INSTANCE;
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.test(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the char function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R> CharFunction<T> map(Function<T, R> f, CharFunction<R> g) {
        return new CharMap<>(f, g);
    }

    char applyAsChar(T value);

    @Override
    default CharType returnType() {
        return Type.charType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    @Override
    default CharFunction<T> mapInput(Function<T, T> f) {
        return x -> applyAsChar(f.apply(x));
    }

    @FunctionalInterface
    interface CharToObject<R> {
        R apply(char value);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.apply(this.applyAsChar(x))}.
     *
     * @param g the outer function
     * @return the object function
     */
    default <R> ObjectFunction<T, R> mapObj(CharToObject<R> g, GenericType<R> returnType) {
        return new CharToObjectMap<>(this, g, returnType);
    }
}
