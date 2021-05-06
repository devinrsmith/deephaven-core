package io.deephaven.logicgate.circuit;

import io.deephaven.db.tables.Table;

/**
 * @see <a href="https://en.wikipedia.org/wiki/Adder_(electronics)">Adder</a>
 */
public interface FullAdder64BitBuilder {

    FullAdder64Bit build(Bits64 a, Bits64 b, Table cIn);
}
