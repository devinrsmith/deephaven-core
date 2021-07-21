package io.deephaven.logicgate.circuit;

import io.deephaven.qst.table.Table;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class FullAdder64BitBuilderImpl implements FullAdder64BitBuilder {

    public abstract FullAdder16BitBuilder adder();

    @Override
    public final FullAdder64Bit build(Bits64 a, Bits64 b, Table cIn) {
        FullAdder16Bit block0 = adder().build(a.b0(), b.b0(), cIn);
        FullAdder16Bit block16 = adder().build(a.b16(), b.b16(), block0.cOut());
        FullAdder16Bit block32 = adder().build(a.b32(), b.b32(), block16.cOut());
        FullAdder16Bit block48 = adder().build(a.b48(), b.b48(), block32.cOut());
        return ImmutableFullAdder64Bit.builder().aIn(a).bIn(b).cIn(cIn).cOut(block48.cOut())
            .s(ImmutableBits64.builder().b0(block0.s()).b16(block16.s()).b32(block32.s())
                .b48(block48.s()).build())
            .build();
    }
}
