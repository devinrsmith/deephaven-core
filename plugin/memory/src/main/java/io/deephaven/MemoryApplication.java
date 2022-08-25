package io.deephaven;

import com.google.auto.service.AutoService;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Listener;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedMutableTable;
import io.deephaven.engine.table.impl.util.RuntimeMemory;
import io.deephaven.engine.table.impl.util.RuntimeMemory.Sample;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.util.SafeCloseable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.Arrays;

@AutoService(ApplicationState.Factory.class)
public final class MemoryApplication implements ApplicationState.Factory {

    private static final ColumnHeader<Instant> TIMESTAMP = ColumnHeader.ofInstant("Timestamp");
    private static final ColumnHeader<Long> FREE_MEMORY = ColumnHeader.ofLong("FreeMemory");
    private static final ColumnHeader<Long> TOTAL_MEMORY = ColumnHeader.ofLong("TotalMemory");
    private static final ColumnHeader<Long> COLLECTION_COUNT = ColumnHeader.ofLong("CollectionCount");
    private static final ColumnHeader<Long> COLLECTION_TIME = ColumnHeader.ofLong("CollectionTime");

    private final RuntimeMemory runtimeMemory;
    private MutableInputTable table;

    public MemoryApplication() {
        this.runtimeMemory = RuntimeMemory.getInstance();
    }

    @Override
    public ApplicationState create(Listener listener) {
        final ApplicationState state =
                new ApplicationState(listener, MemoryApplication.class.getName(), "Memory Application");
        final LivenessScope scope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            table = createMemory(state);
        }
        // todo: have a better way to do this
        new Thread(() -> {
            final Sample buf = new Sample();
            while (true) {
                runtimeMemory.read(buf);
                try {
                    table.add(InMemoryTable.from(
                            ColumnHeader.of(TIMESTAMP, FREE_MEMORY, TOTAL_MEMORY, COLLECTION_COUNT, COLLECTION_TIME)
                                    .start(1)
                                    .row(Instant.ofEpochMilli(buf.timestampMillis), buf.freeMemory, buf.totalMemory,
                                            buf.totalCollections, buf.totalCollectionTimeMs)
                                    .newTable()));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "MemoryApplication-thread").start();
        return state;
    }

    private MutableInputTable createMemory(ApplicationState state) {
        final AppendOnlyArrayBackedMutableTable n = AppendOnlyArrayBackedMutableTable.make(TableDefinition
                .from(Arrays.asList(TIMESTAMP, FREE_MEMORY, TOTAL_MEMORY, COLLECTION_COUNT, COLLECTION_TIME)));
        state.setField("memory", n.readOnlyCopy());
        return (MutableInputTable) n.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
    }
}
