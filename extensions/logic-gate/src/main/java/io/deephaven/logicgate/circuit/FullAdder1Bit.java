package io.deephaven.logicgate.circuit;

import io.deephaven.qst.table.Table;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class FullAdder1Bit {

    public abstract Table a();

    public abstract Table b();

    public abstract Table cIn();

    public abstract Table cOut();

    public abstract Table s();
}
