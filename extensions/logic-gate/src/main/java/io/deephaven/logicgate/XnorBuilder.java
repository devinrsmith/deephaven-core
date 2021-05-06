package io.deephaven.logicgate;

import io.deephaven.db.tables.Table;

public interface XnorBuilder {
    Table xnor(Table a, Table b);
}
