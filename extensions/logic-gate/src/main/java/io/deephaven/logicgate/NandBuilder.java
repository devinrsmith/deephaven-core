package io.deephaven.logicgate;

import io.deephaven.qst.table.Table;

public interface NandBuilder {
    Table nand(Table a, Table b);
}
