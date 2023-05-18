/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.sql;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.TableCreatorImpl;
import io.deephaven.engine.table.impl.UpdatableTable;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.InputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeTable;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

class SqlScanSupport implements TableCreator<Table> {

    private final Map<String, Table> scope;

    SqlScanSupport(Map<String, Table> scope) {
        this.scope = Objects.requireNonNull(scope);
    }

    @Override
    public Table of(TicketTable ticketTable) {
        final String scanTicket = new String(ticketTable.ticket(), StandardCharsets.UTF_8);
        if (!scanTicket.startsWith("scan/")) {
            throw new IllegalStateException();
        }
        return scope.get(scanTicket.substring(5).toLowerCase());
    }

    @Override
    public final Table of(NewTable newTable) {
        return TableCreatorImpl.INSTANCE.of(newTable);
    }

    @Override
    public final Table of(EmptyTable emptyTable) {
        return TableCreatorImpl.INSTANCE.of(emptyTable);
    }

    @Override
    public final Table of(TimeTable timeTable) {
        return TableCreatorImpl.INSTANCE.of(timeTable);
    }

    @Override
    public final UpdatableTable of(InputTable inputTable) {
        return TableCreatorImpl.INSTANCE.of(inputTable);
    }

    @Override
    public final Table merge(Iterable<Table> tables) {
        return TableCreatorImpl.INSTANCE.merge(tables);
    }
}
