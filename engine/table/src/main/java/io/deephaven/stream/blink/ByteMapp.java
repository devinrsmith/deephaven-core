package io.deephaven.stream.blink;

import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.Type;

@FunctionalInterface
public interface ByteMapp<T> extends Mapp<T> {

    byte applyAsByte(T value);

    @Override
    default ByteType returnType() {
        return Type.byteType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
