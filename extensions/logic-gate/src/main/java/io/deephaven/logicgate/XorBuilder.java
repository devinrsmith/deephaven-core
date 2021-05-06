package io.deephaven.logicgate;

import io.deephaven.db.tables.Table;

public interface XorBuilder {
    Table xor(Table a, Table b);
}
