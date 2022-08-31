package io.deephaven;

import com.google.auto.service.AutoService;
import com.sun.management.GarbageCollectionNotificationInfo;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Listener;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.util.SafeCloseable;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.time.Instant;

import static com.sun.management.GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION;

// @AutoService(ApplicationState.Factory.class)
public final class GcApplication implements ApplicationState.Factory, NotificationListener {

    private static final ColumnHeader<Long> ID = ColumnHeader.ofLong("Id");
    private static final ColumnHeader<Instant> START = ColumnHeader.ofInstant("Start");
    private static final ColumnHeader<Instant> END = ColumnHeader.ofInstant("End");
    private static final ColumnHeader<String> GC_NAME = ColumnHeader.ofString("GcName");
    private static final ColumnHeader<String> GC_ACTION = ColumnHeader.ofString("GcAction");
    private static final ColumnHeader<String> GC_CAUSE = ColumnHeader.ofString("GcCause");
    private static final ColumnHeader<Long> BEFORE_GC = ColumnHeader.ofLong("BeforeGc");
    private static final ColumnHeader<Long> AFTER_GC = ColumnHeader.ofLong("AfterGc");

    private static final ColumnHeader<String> POOL = ColumnHeader.ofString("Pool");
    private static final ColumnHeader<Long> INIT = ColumnHeader.ofLong("Init");
    private static final ColumnHeader<Long> USED = ColumnHeader.ofLong("Used");
    private static final ColumnHeader<Long> MAX = ColumnHeader.ofLong("Max");
    private static final ColumnHeader<Long> COMMITTED = ColumnHeader.ofLong("Committed");

    private GcNotificationStream gcNotificationStream;


    @Override
    public ApplicationState create(Listener listener) {
        final ApplicationState state =
                new ApplicationState(listener, GcApplication.class.getName(), "Garbage-Collection Application");
        // todo: should we be managing this at the application state level?
        // io.deephaven.engine.liveness.LivenessManager
        final LivenessScope scope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            gcNotificationStream = GcNotificationStream.of("notification_info", UpdateGraphProcessor.DEFAULT, null);
            state.setField("notification_info", gcNotificationStream.table());
        }
        install();
        return state;
    }

    // private TableToStream createNotificationInfo(ApplicationState state) {
    // final TableDefinition x = TableDefinition.from(Arrays.asList(ID, START, END, GC_NAME, GC_ACTION, GC_CAUSE,
    // BEFORE_GC, AFTER_GC));
    // final TableToStream tts = TableToStream.of("notification_info", x, UpdateGraphProcessor.DEFAULT, null, null);
    // final Table streamTable = tts.table();
    // state.setField("notification_info", streamTable);
    // state.setField("notification_info_append", StreamTableTools.streamToAppendOnlyTable(streamTable));
    // return tts;
    // }

    // private TableToStream createBeforePools(ApplicationState state) {
    // final TableToStream tts =
    // TableToStream.of(TableDefinition.from(Arrays.asList(ID, POOL, INIT, USED, COMMITTED, MAX)));
    // state.setField("before_pools", tts.table());
    // state.setField("before_pools_append", StreamTableTools.streamToAppendOnlyTable(tts.table()));
    // return tts;
    // }
    //
    // private TableToStream createAfterPools(ApplicationState state) {
    // final TableToStream tts =
    // TableToStream.of(TableDefinition.from(Arrays.asList(ID, POOL, INIT, USED, COMMITTED, MAX)));
    // state.setField("after_pools", tts.table());
    // state.setField("after_pools_append", StreamTableTools.streamToAppendOnlyTable(tts.table()));
    // return tts;
    // }

    public void install() {
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!(bean instanceof NotificationBroadcaster)) {
                continue;
            }
            ((NotificationBroadcaster) bean).addNotificationListener(this, null, null);
        }
    }

    public void remove() throws ListenerNotFoundException {
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!(bean instanceof NotificationBroadcaster)) {
                continue;
            }
            ((NotificationBroadcaster) bean).removeNotificationListener(this);
        }
    }

    @Override
    public void handleNotification(Notification notification, Object handback) {
        if (!GARBAGE_COLLECTION_NOTIFICATION.equals(notification.getType())) {
            return;
        }
        try {
            handleGCInfo(GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void handleGCInfo(GarbageCollectionNotificationInfo info) throws IOException {
        gcNotificationStream.add(info);
        // final long id = info.getGcInfo().getId();
        // final long vmStartOffsetMillis = info.getGcInfo().getStartTime();
        // final long vmEndOffsetMillis = info.getGcInfo().getEndTime();
        // final String gcName = info.getGcName();
        // final String gcAction = info.getGcAction();
        // final String gcCause = info.getGcCause();
        // final Map<String, MemoryUsage> before = info.getGcInfo().getMemoryUsageBeforeGc();
        // final Map<String, MemoryUsage> after = info.getGcInfo().getMemoryUsageAfterGc();
        // final long beforeGc = before.values().stream().mapToLong(MemoryUsage::getUsed).sum();
        // final long afterGc = after.values().stream().mapToLong(MemoryUsage::getUsed).sum();
        //// beforePools.addSplittable(pool(id, before.entrySet()));
        //// afterPools.addSplittable(pool(id, after.entrySet()));
        // notificationInfo.add(InMemoryTable.from(NewTable.of(
        // Column.of(ID, id),
        // Column.of(START, vmStart.plusMillis(vmStartOffsetMillis)),
        // Column.of(END, vmStart.plusMillis(vmEndOffsetMillis)),
        // Column.of(GC_NAME, gcName),
        // Column.of(GC_ACTION, gcAction),
        // Column.of(GC_CAUSE, gcCause),
        // Column.of(BEFORE_GC, beforeGc),
        // Column.of(AFTER_GC, afterGc))));
        // // note: this *CAN* be negative
        // final long reclaimedBytes = beforeGc - afterGc;
        //
        // // There are potentially other interesting stats that can be derived here in the future. The
        // // stats could be broken down by pool name / gc type.
        // final long allocatedBytes = beforeGc - lastAfterGc;
    }

    // private static Table pool(long id, Set<Entry<String, MemoryUsage>> entries) {
    // final ColumnHeaders6<Long, String, Long, Long, Long, Long>.Rows rows =
    // ColumnHeader.of(ID, POOL, INIT, USED, COMMITTED, MAX).start(entries.size());
    // for (Entry<String, MemoryUsage> entry : entries) {
    // final MemoryUsage value = entry.getValue();
    // rows.row(
    // id,
    // entry.getKey(),
    // value.getInit() == -1 ? null : value.getInit(),
    // value.getUsed(),
    // value.getCommitted(),
    // value.getMax() == -1 ? null : value.getMax());
    // }
    // return InMemoryTable.from(rows.newTable());
    // }

}
