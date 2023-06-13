package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

@FunctionalInterface
public interface CharFunction<T> extends TypedFunction<T> {

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

    default CharFunction<T> onNull(char onNull) {
        return x -> x == null ? onNull : applyAsChar(x);
    }
}
