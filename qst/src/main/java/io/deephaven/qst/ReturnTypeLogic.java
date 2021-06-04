package io.deephaven.qst;

import io.deephaven.qst.column.ColumnType;

public interface ReturnTypeLogic<R> {

    <T> R transform(ColumnType<T> inputType, T inputValue);
}
