package io.deephaven.logicgate;

import io.deephaven.qst.table.Table;

public interface NotBuilder {
    Table not(Table a);
}
