package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

@FunctionalInterface
public interface ByteFunction<T> extends TypedFunction<T> {

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

    default ByteFunction<T> onNull(byte onNull) {
        return x -> x == null ? onNull : applyAsByte(x);
    }
}
