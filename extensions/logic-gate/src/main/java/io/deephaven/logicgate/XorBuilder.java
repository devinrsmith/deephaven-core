package io.deephaven.logicgate;

import io.deephaven.qst.table.Table;

public interface XorBuilder {
    Table xor(Table a, Table b);
}
