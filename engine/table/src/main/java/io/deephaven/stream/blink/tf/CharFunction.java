package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.util.function.Function;

@FunctionalInterface
public interface CharFunction<T> extends TypedFunction<T> {
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
     * Assumes the object value is {@code null} or directly castable to a char. On null, the function returns {@link QueryConstants#NULL_CHAR}. Equivalent to {@code NullGuard.of(primitive())}.
     *
     * @return the guarded char function
     * @param <T> the value type
     * @see #primitive()
     * @see NullGuard#of(CharFunction)
     */
    static <T> CharFunction<T> guardedPrimitive() {
        //noinspection unchecked
        return (CharFunction<T>) Functions.CHAR_GUARDED;
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

    /**
     * Create a new function which returns {@code onNull} when the value is {@code null}, and otherwise calls {@link #applyAsChar(Object)}. Equivalent to {@code x -> x == null ? onNull : applyAsChar(x)}.
     * @param onNull the value to return on null
     * @return the new char function
     */
    default CharFunction<T> onNullInput(char onNull) {
        return x -> x == null ? onNull : applyAsChar(x);
    }
}
