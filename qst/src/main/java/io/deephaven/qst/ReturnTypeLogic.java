package io.deephaven.qst;

public interface ReturnTypeLogic<R> {

    <T> R transform(ColumnType<T> inputType, T inputValue);
}
