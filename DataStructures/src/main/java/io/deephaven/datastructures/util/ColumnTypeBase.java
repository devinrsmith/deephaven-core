package io.deephaven.datastructures.util;

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
}
