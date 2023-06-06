package io.deephaven.app.f;

@FunctionalInterface
public interface ShortMapp<T> extends Mapp<T> {

    short applyAsShort(T value);

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
