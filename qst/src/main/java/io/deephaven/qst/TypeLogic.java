package io.deephaven.qst;

import io.deephaven.qst.column.type.ColumnType;
import java.util.Iterator;
import java.util.stream.StreamSupport;

public interface TypeLogic {

    <T, R> R transform(ColumnType<R> returnType, ColumnType<T> inputType, T inputValue);

    default <T, R> Iterable<R> transform(ColumnType<R> returnType, ColumnType<T> inputType, Iterable<T> fromValues) {
        return () -> new Iterator<R>() {
            private final Iterator<T> it = fromValues.iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public R next() {
                return transform(returnType, inputType, it.next());
            }
        };
    }

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
