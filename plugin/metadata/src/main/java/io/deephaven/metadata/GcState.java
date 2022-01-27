package io.deephaven.metadata;

import com.google.auto.service.AutoService;
import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedMutableTable;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.plugin.app.State;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders6;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;

@AutoService(State.class)
public final class GcState implements State, NotificationListener {

    public static final ColumnHeaders6<String, String, String, Long, Instant, Instant> EVENTS_HEADER =
            ColumnHeader.ofString("Name")
                    .header(ColumnHeader.ofString("Action"))
                    .header(ColumnHeader.ofString("Cause"))
                    .header(ColumnHeader.ofLong("Id"))
                    .header(ColumnHeader.ofInstant("Start"))
                    .header(ColumnHeader.ofInstant("End"));

    public static final ColumnHeaders6<String, Long, Long, Long, Long, Long> POOL_HEADER = ColumnHeader.ofString("Pool")
            .header(ColumnHeader.ofLong("Id"))
            .header(ColumnHeader.ofLong("Init"))
            .header(ColumnHeader.ofLong("Used"))
            .header(ColumnHeader.ofLong("Committed"))
            .header(ColumnHeader.ofLong("Max"));

    private final Instant startTime;

    private MutableInputTable eventsInput;
    private Table events;

    private MutableInputTable beforeInput;
    private Table before;

    private MutableInputTable afterInput;
    private Table after;

    private final Table combined;

    private static void createInputTable(Iterable<ColumnHeader<?>> headers,
            java.util.function.Consumer<MutableInputTable> c1,
            java.util.function.Consumer<Table> c2) {
        final AppendOnlyArrayBackedMutableTable table =
                AppendOnlyArrayBackedMutableTable.make(TableDefinition.from(headers));
        MutableInputTable t1 = (MutableInputTable) table.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        Table t2 = table.copy(false);
        table.copyAttributes(t2, x -> !Table.INPUT_TABLE_ATTRIBUTE.equals(x));
        c1.accept(t1);
        c2.accept(t2);
    }


    public GcState() {
        startTime = Instant.ofEpochMilli(ManagementFactory.getRuntimeMXBean().getStartTime());

        createInputTable(EVENTS_HEADER, table -> GcState.this.eventsInput = table,
                table -> GcState.this.events = table);
        createInputTable(POOL_HEADER, table -> GcState.this.beforeInput = table,
                table -> GcState.this.before = table);
        createInputTable(POOL_HEADER, table -> GcState.this.afterInput = table,
                table -> GcState.this.after = table);

        combined = after.view("Id", "Pool", "AfterUsed=Used", "AfterCommitted=Committed")
                .naturalJoin(before, "Id,Pool", "BeforeUsed=Used,BeforeCommitted=Committed")
                .naturalJoin(events, "Id");

        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            final NotificationEmitter emitter = (NotificationEmitter) gc;
            emitter.addNotificationListener(GcState.this, null, null);
        }
    }

    @Override
    public void handleNotification(Notification notification, Object handback) {
        if (GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(notification.getType())) {
            handleGc(GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData()));
        }
    }

    private void handleGc(GarbageCollectionNotificationInfo gc) {
        final GcInfo info = gc.getGcInfo();
        try {
            eventsInput.add(event(gc));
            beforeInput.add(pool(info, info.getMemoryUsageBeforeGc()));
            afterInput.add(pool(info, info.getMemoryUsageAfterGc()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Table event(GarbageCollectionNotificationInfo gc) {
        final GcInfo info = gc.getGcInfo();
        final Instant start = startTime.plusMillis(info.getStartTime());
        final Instant end = startTime.plusMillis(info.getEndTime());
        return InMemoryTable.from(EVENTS_HEADER.start(1)
                .row(gc.getGcName(), gc.getGcAction(), gc.getGcCause(), info.getId(), start, end)
                .newTable());
    }

    private static Table pool(GcInfo info, Map<String, MemoryUsage> pool) {
        final ColumnHeaders6<String, Long, Long, Long, Long, Long>.Rows rows = POOL_HEADER.start(pool.size());
        for (Entry<String, MemoryUsage> e : pool.entrySet()) {
            final long init = e.getValue().getInit();
            final long used = e.getValue().getUsed();
            final long committed = e.getValue().getCommitted();
            final long max = e.getValue().getMax();
            rows.row(e.getKey(), info.getId(), init == -1 ? null : init, used, committed, max == -1 ? null : max);
        }
        return InMemoryTable.from(rows.newTable());
    }

    @Override
    public void insertInto(Consumer consumer) {
        consumer.set("events", events, "The GC events");
        consumer.set("before", before, "The before pools");
        consumer.set("after", after, "The after pools");
        consumer.set("combined", combined, "The combined");
    }
}
