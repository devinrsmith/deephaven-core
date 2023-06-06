package io.deephaven.app.f;

@FunctionalInterface
public interface FloatMapp<T> extends Mapp<T> {

    float applyAsFloat(T value);

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
