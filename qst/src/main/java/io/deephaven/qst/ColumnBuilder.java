package io.deephaven.qst;

public interface ColumnBuilder<T> {

  ColumnBuilder<T> add(T item);

  Column<T> build();
}
