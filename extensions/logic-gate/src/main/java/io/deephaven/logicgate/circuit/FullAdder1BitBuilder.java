package io.deephaven.logicgate.circuit;

import io.deephaven.db.tables.Table;

/**
 * @see <a href="https://en.wikipedia.org/wiki/Adder_(electronics)">Adder</a>
 */
public interface FullAdder1BitBuilder {

    FullAdder1Bit build(Table a, Table b, Table cIn);

    default FullAdder4BitBuilder to4Bits() {
        return ImmutableFullAdder4BitBuilderImpl.builder().adder(this).build();
    }
}
