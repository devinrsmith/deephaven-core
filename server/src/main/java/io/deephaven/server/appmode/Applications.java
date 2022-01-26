package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedMutableTable;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders2;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

final class Applications implements ApplicationStates {

    private static final ColumnHeaders2<String, String> HEADER = ColumnHeader.ofString("Id")
            .header(ColumnHeader.ofString("Name"));

    public static Applications create() {
        final AppendOnlyArrayBackedMutableTable table =
                AppendOnlyArrayBackedMutableTable.make(TableDefinition.from(HEADER));
        final MutableInputTable inputTable = (MutableInputTable) table.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        final Table readOnlyTable = table.copy(false);
        table.copyAttributes(readOnlyTable, x -> !Table.INPUT_TABLE_ATTRIBUTE.equals(x));
        return new Applications(new ConcurrentHashMap<>(), inputTable, readOnlyTable);
    }

    private final Map<String, ApplicationState> applicationMap;
    private final MutableInputTable inputTable;
    private final Table readOnlyTable;

    private Applications(Map<String, ApplicationState> applicationMap, MutableInputTable inputTable,
            Table readOnlyTable) {
        this.applicationMap = Objects.requireNonNull(applicationMap);
        this.inputTable = Objects.requireNonNull(inputTable);
        this.readOnlyTable = Objects.requireNonNull(readOnlyTable);
    }

    public synchronized void onApplicationLoad(final ApplicationState app) {
        if (applicationMap.containsKey(app.id())) {
            if (applicationMap.get(app.id()) != app) {
                throw new IllegalArgumentException("Duplicate application found for app_id " + app.id());
            }
            return;
        }
        applicationMap.put(app.id(), app);
        final String id = app.id();
        final String name = app.name();
        try {
            inputTable.add(InMemoryTable.from(HEADER.start(1).row(id, name).newTable()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Optional<ApplicationState> getApplicationState(String applicationId) {
        return Optional.ofNullable(applicationMap.get(applicationId));
    }

    @Override
    public Collection<ApplicationState> values() {
        return Collections.unmodifiableCollection(applicationMap.values());
    }

    public Table table() {
        return readOnlyTable;
    }
}
