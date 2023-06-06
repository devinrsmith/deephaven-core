package io.deephaven.app.f;

@FunctionalInterface
public interface ByteMapp<T> extends Mapp<T> {

    byte applyAsByte(T value);

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}
