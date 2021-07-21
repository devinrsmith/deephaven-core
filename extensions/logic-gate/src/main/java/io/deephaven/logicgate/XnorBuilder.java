package io.deephaven.logicgate;

import io.deephaven.qst.table.Table;

public interface XnorBuilder {
    Table xnor(Table a, Table b);
}
