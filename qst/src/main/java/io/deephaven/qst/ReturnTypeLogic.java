package io.deephaven.qst;

import io.deephaven.qst.table.column.type.ColumnType;

public interface ReturnTypeLogic<R> {

    <T> R transform(ColumnType<T> inputType, T inputValue);
}
