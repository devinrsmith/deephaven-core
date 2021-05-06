package io.deephaven.logicgate.circuit;

import io.deephaven.db.tables.Table;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class FullAdder64Bit {

    public abstract Bits64 aIn();

    public abstract Bits64 bIn();

    public abstract Table cIn();

    public abstract Table cOut();

    public abstract Bits64 s();
}
