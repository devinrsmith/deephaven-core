package io.deephaven.app.f;

import java.util.function.Function;

@FunctionalInterface
public interface ObjectMapp<T, R> extends Mapp<T>, Function<T, R> {

    @Override
    default <Z> Z walk(Visitor<T, Z> visitor) {
        return visitor.visit(this);
    }
}
