package io.deephaven.logicgate;

import io.deephaven.db.tables.Table;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class Nand {
    public abstract Table a();

    public abstract Table b();

    public abstract Table q();
}
