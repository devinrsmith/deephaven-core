package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.util.function.Function;

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