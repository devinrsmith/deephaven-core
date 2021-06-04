package io.deephaven.qst.logic;

import io.deephaven.qst.ColumnType;
import io.deephaven.qst.ReturnTypeLogic;

public enum StringLogic implements ReturnTypeLogic<String> {
    INSTANCE;

    public static StringLogic instance() {
        return INSTANCE;
    }

    @Override
    public final <T> String transform(ColumnType<T> inputType, T inputValue) {
        return inputValue == null ? null : inputValue.toString();
    }
}
