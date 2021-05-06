package io.deephaven.logicgate;

import io.deephaven.db.tables.Table;

public interface NorBuilder {
    Table nor(Table a, Table b);
}
