package io.deephaven.logicgate.circuit;

import io.deephaven.db.tables.Table;

/**
 * @see <a href="https://en.wikipedia.org/wiki/Adder_(electronics)">Adder</a>
 */
public interface FullAdder4BitBuilder {

    FullAdder4Bit build(Bits4 a, Bits4 b, Table cIn);

    default FullAdder16BitBuilder to16Bits() {
        return ImmutableFullAdder16BitBuilderImpl.builder().adder(this).build();
    }
}
