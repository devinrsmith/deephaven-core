package io.deephaven.qst.table;

import java.util.List;

/**
 * Implementation specific table spec - most visitors should throw an exception when encountering this.
 */
public abstract class ImplementationTable extends TableBase {

    public abstract List<TableSpec> parents();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
