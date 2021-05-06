package io.deephaven.logicgate;

import io.deephaven.db.tables.Table;

public interface AndBuilder {
    Table and(Table a, Table b);
}
