package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.Type;

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
}
