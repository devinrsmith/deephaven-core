package io.deephaven.logicgate;

import io.deephaven.db.tables.Table;

public interface NotBuilder {
    Table not(Table a);
}
