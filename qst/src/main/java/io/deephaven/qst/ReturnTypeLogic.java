package io.deephaven.qst;

import io.deephaven.qst.column.type.ColumnType;

public interface ReturnTypeLogic<R> {

    <T> R transform(ColumnType<T> inputType, T inputValue);
}
