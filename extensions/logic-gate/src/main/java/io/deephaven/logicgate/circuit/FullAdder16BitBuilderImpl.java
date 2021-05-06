package io.deephaven.logicgate.circuit;

import io.deephaven.db.tables.Table;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class FullAdder16BitBuilderImpl implements FullAdder16BitBuilder {

    public abstract FullAdder4BitBuilder adder();

    @Override
    public final FullAdder16Bit build(Bits16 a, Bits16 b, Table cIn) {
        FullAdder4Bit block0 = adder().build(a.b0(), b.b0(), cIn);
        FullAdder4Bit block4 = adder().build(a.b4(), b.b4(), block0.cOut());
        FullAdder4Bit block8 = adder().build(a.b8(), b.b8(), block4.cOut());
        FullAdder4Bit block12 = adder().build(a.b12(), b.b12(), block8.cOut());
        return ImmutableFullAdder16Bit.builder().aIn(a).bIn(b).cIn(cIn).cOut(block12.cOut())
            .s(ImmutableBits16.builder().b0(block0.s()).b4(block4.s()).b8(block8.s())
                .b12(block12.s()).build())
            .build();
    }
}
