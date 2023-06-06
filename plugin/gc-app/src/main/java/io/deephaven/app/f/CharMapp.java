package io.deephaven.app.f;

@FunctionalInterface
public interface CharMapp<T> extends Mapp<T> {

    char applyAsChar(T value);

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
