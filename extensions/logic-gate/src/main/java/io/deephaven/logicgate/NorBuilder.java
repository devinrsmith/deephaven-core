package io.deephaven.logicgate;

import io.deephaven.qst.table.Table;

public interface NorBuilder {
    Table nor(Table a, Table b);
}
