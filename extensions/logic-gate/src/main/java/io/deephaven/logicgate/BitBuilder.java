package io.deephaven.logicgate;

import io.deephaven.db.tables.Table;

public interface BitBuilder {
    Table zero();

    Table one();
    // todo: flipable bit

    SettableBit settable();
}
