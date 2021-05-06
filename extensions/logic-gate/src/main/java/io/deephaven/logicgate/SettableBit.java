package io.deephaven.logicgate;

import io.deephaven.db.tables.Table;

public interface SettableBit {
    Table bit();

    void clear();

    void set();
}
