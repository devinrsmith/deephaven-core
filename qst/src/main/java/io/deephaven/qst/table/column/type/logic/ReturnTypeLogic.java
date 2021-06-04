package io.deephaven.qst.table.column.type.logic;

import io.deephaven.qst.table.column.type.ColumnType;

public interface ReturnTypeLogic<R> {

    <T> R transform(ColumnType<T> inputType, T inputValue);
}
