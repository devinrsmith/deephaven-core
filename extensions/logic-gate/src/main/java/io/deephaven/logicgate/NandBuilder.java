package io.deephaven.logicgate;

import io.deephaven.db.tables.Table;

public interface NandBuilder {
    Table nand(Table a, Table b);
}
