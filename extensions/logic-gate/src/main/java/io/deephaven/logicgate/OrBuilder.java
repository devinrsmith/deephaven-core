package io.deephaven.logicgate;

import io.deephaven.qst.table.Table;

public interface OrBuilder {
    Table or(Table a, Table b);
}
