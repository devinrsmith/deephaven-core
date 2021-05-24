package io.deephaven.qst;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public abstract class ColumnTypeBase<T> implements ColumnType<T> {

    private final List<Class<T>> classes;

    ColumnTypeBase(List<Class<T>> classes) {
        this.classes = Objects.requireNonNull(classes);
        if (classes.isEmpty()) {
            throw new IllegalArgumentException("Classes must not be empty");
        }
    }

    @Override
    public final Stream<Class<T>> classes() {
        return classes.stream();
    }

    @Override
    public final Column<T> cast(Column<?> column) {
        //noinspection unchecked
        return (Column<T>)column;
    }

    @Override
    public final ColumnHeader<T> cast(ColumnHeader<?> columnHeader) {
        //noinspection unchecked
        return (ColumnHeader<T>)columnHeader;
    }

    @Override
    public final T castValue(Object value) {
        //noinspection unchecked
        return (T)value;
    }

    @Override
    public final <R> Iterable<T> transformValues(TypeLogic logic, ColumnType<R> fromType, Iterable<R> fromValues) {
        return () -> new Iterator<T>() {
            private final Iterator<R> it = fromValues.iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public T next() {
                return logic.transform(ColumnTypeBase.this, fromType, it.next());
            }
        };
    }
}
