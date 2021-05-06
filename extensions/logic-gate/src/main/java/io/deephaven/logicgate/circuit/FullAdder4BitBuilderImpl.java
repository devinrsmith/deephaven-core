package io.deephaven.logicgate.circuit;

import io.deephaven.db.tables.Table;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class FullAdder4BitBuilderImpl implements FullAdder4BitBuilder {

    public abstract FullAdder1BitBuilder adder();

    @Override
    public final FullAdder4Bit build(Bits4 a, Bits4 b, Table cIn) {
        FullAdder1Bit block0 = adder().build(a.b0(), b.b0(), cIn);
        FullAdder1Bit block1 = adder().build(a.b1(), b.b1(), block0.cOut());
        FullAdder1Bit block2 = adder().build(a.b2(), b.b2(), block1.cOut());
        FullAdder1Bit block3 = adder().build(a.b3(), b.b3(), block2.cOut());
        return ImmutableFullAdder4Bit.builder().aIn(a).bIn(b).cIn(cIn).cOut(block3.cOut())
            .s(ImmutableBits4.builder().b0(block0.s()).b1(block1.s()).b2(block2.s()).b3(block3.s())
                .build())
            .build();
    }
}
