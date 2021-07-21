package io.deephaven.logicgate.circuit;

import io.deephaven.qst.table.Table;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class HalfAdder {

    public abstract Table a();

    public abstract Table b();

    public abstract Table c();

    public abstract Table s();
}
