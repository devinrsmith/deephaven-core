package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.Type;

import java.util.function.Function;

@FunctionalInterface
public interface BoxedBooleanFunction<T> extends TypedFunction<T> {

    Boolean applyAsBoolean(T value);

    @Override
    default BooleanType returnType() {
        return Type.booleanType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    @Override
    default BoxedBooleanFunction<T> mapInput(Function<T, T> f) {
        return x -> applyAsBoolean(f.apply(x));
    }

    default BoxedBooleanFunction<T> onNullInput(Boolean onNull) {
        return x -> x == null ? onNull : applyAsBoolean(x);
    }
}
