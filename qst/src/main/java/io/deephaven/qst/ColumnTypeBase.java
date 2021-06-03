package io.deephaven.qst;

public abstract class ColumnTypeBase<T> implements ColumnType<T> {

    @Override
    public final T castValue(Object value) {
        //noinspection unchecked
        return (T)value;
    }
}
