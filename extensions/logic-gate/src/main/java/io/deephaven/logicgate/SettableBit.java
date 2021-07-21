package io.deephaven.logicgate;

import io.deephaven.qst.table.Table;

public interface SettableBit {
    Table bit();

    void clear();

    void set();
}
