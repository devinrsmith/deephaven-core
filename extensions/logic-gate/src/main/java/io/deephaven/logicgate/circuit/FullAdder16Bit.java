package io.deephaven.logicgate.circuit;

import io.deephaven.qst.table.Table;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class FullAdder16Bit {

    public abstract Bits16 aIn();

    public abstract Bits16 bIn();

    public abstract Table cIn();

    public abstract Table cOut();

    public abstract Bits16 s();
}
