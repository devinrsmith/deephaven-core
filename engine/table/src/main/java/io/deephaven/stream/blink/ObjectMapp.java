package io.deephaven.stream.blink;

import io.deephaven.qst.type.Type;

import java.util.function.Function;

public interface ObjectMapp<T, R> extends Mapp<T> {

    static <T, R> ObjectMapp<T, R> of(Function<T, R> f, Type<R> returnType) {
        return new ObjectMapp<>() {
            @Override
            public Type<R> returnType() {
                return returnType;
            }

            @Override
            public R apply(T value) {
                return f.apply(value);
            }
        };
    }

    Type<R> returnType();

    R apply(T value);

    @Override
    default <Z> Z walk(Visitor<T, Z> visitor) {
        return visitor.visit(this);
    }
}
