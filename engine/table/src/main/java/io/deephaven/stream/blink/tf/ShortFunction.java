package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

@FunctionalInterface
public interface ShortFunction<T> extends TypedFunction<T> {

    short applyAsShort(T value);

    @Override
    default ShortType returnType() {
        return Type.shortType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    @Override
    default ShortFunction<T> mapInput(Function<T, T> f) {
        return x -> applyAsShort(f.apply(x));
    }

    default ShortFunction<T> onNullInput(short onNull) {
        return x -> x == null ? onNull : applyAsShort(x);
    }
}
