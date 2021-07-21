package io.deephaven.logicgate.circuit;

import io.deephaven.qst.table.Table;

/**
 * @see <a href="https://en.wikipedia.org/wiki/Adder_(electronics)">Adder</a>
 */
public interface FullAdder16BitBuilder {

    FullAdder16Bit build(Bits16 a, Bits16 b, Table cIn);

    default FullAdder64BitBuilder to64Bits() {
        return ImmutableFullAdder64BitBuilderImpl.builder().adder(this).build();
    }
}
