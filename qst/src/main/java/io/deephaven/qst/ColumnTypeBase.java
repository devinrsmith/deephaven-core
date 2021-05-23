package io.deephaven.qst;

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
}
