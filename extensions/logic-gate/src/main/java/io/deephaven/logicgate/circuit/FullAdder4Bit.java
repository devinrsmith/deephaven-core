package io.deephaven.logicgate.circuit;

import io.deephaven.qst.table.Table;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class FullAdder4Bit {

    public abstract Bits4 aIn();

    public abstract Bits4 bIn();

    public abstract Table cIn();

    public abstract Table cOut();

    public abstract Bits4 s();
}
