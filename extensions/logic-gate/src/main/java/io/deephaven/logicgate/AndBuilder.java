package io.deephaven.logicgate;

import io.deephaven.qst.table.Table;

public interface AndBuilder {
    Table and(Table a, Table b);
}
