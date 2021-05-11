package io.deephaven.logicgate;

import io.deephaven.db.tables.Table;
import java.time.Duration;

public interface BitBuilder {
    Table zero();

    Table one();
    // todo: flipable bit

    SettableBit settable();

    Table timedBit(Duration duration);
}
