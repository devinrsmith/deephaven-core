package io.deephaven.stream.blink;

import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.Type;

@FunctionalInterface
public interface CharMapp<T> extends Mapp<T> {

    char applyAsChar(T value);

    @Override
    default CharType returnType() {
        return Type.charType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
