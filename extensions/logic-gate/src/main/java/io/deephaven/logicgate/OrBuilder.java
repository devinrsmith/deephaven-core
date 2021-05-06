package io.deephaven.logicgate;

import io.deephaven.db.tables.Table;

public interface OrBuilder {
    Table or(Table a, Table b);
}
