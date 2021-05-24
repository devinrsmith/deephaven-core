package io.deephaven.qst;

import java.util.stream.StreamSupport;

public interface TypeLogic {

    <T, R> R transform(ColumnType<R> returnType, ColumnType<T> inputType, T inputValue);

    default <T> boolean canTransform(ColumnType<?> returnType, ColumnType<T> inputType, T inputValue) {
        try {
            transform(returnType, inputType, inputValue);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    default <T> boolean canTransform(ColumnType<?> returnType, ColumnType<T> inputType, Iterable<T> inputValues) {
        return StreamSupport
            .stream(inputValues.spliterator(), false)
            .allMatch(x -> canTransform(returnType, inputType, x));
    }
}
