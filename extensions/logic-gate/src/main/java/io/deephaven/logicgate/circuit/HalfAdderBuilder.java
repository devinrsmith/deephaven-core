package io.deephaven.logicgate.circuit;

import io.deephaven.qst.table.Table;

/**
 * @see <a href="https://en.wikipedia.org/wiki/Adder_(electronics)">Adder</a>
 */
public interface HalfAdderBuilder {

    HalfAdder build(Table a, Table b);
}
