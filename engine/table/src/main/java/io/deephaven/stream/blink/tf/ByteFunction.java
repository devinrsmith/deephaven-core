package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.util.function.Function;

@FunctionalInterface
public interface ByteFunction<T> extends TypedFunction<T> {

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

    /**
     * Assumes the object value is {@code null} or directly castable to a byte. On null, the function returns {@link QueryConstants#NULL_BYTE}. Equivalent to {@code NullGuard.of(primitive())}.
     *
     * @return the guarded byte function
     * @param <T> the value type
     * @see #primitive()
     * @see NullGuard#of(ByteFunction)
     */
    static <T> ByteFunction<T> guardedPrimitive() {
        //noinspection unchecked
        return (ByteFunction<T>) Functions.BYTE_GUARDED;
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

    /**
     * Create a new function which returns {@code onNull} when the value is {@code null}, and otherwise calls {@link #applyAsByte(Object)}. Equivalent to {@code x -> x == null ? onNull : applyAsByte(x)}.
     * @param onNull the value to return on null
     * @return the new byte function
     */
    default ByteFunction<T> onNullInput(byte onNull) {
        return x -> x == null ? onNull : applyAsByte(x);
    }
}
