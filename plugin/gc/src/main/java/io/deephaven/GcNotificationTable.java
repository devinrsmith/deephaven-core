package io.deephaven;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.DateTimeArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.time.DateTimeUtils;

import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;

final class GcNotificationTable {

    public static TableDefinition definition() {
        return TableDefinition.of(
                ColumnDefinition.ofLong("Id"),
                ColumnDefinition.ofTime("Start"),
                ColumnDefinition.ofTime("End"),
                ColumnDefinition.ofString("GcName"),
                ColumnDefinition.ofString("GcAction"),
                ColumnDefinition.ofString("GcCause"));
    }

    static final int BUFFER_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    private final long vmStartMillis;
    private final LongArraySource id;
    private final DateTimeArraySource start;
    private final DateTimeArraySource end;
    private final ObjectArraySource<String> gcName;
    private final ObjectArraySource<String> gcAction;
    private final ObjectArraySource<String> gcCause;
    private final Map<String, ColumnSource<?>> columns;
    private long ix;

    public GcNotificationTable() {
        vmStartMillis = ManagementFactory.getRuntimeMXBean().getStartTime();
        id = new LongArraySource();
        start = new DateTimeArraySource();
        end = new DateTimeArraySource();
        gcName = new ObjectArraySource<>(String.class);
        gcAction = new ObjectArraySource<>(String.class);
        gcCause = new ObjectArraySource<>(String.class);

        id.ensureCapacity(BUFFER_SIZE);
        start.ensureCapacity(BUFFER_SIZE);
        end.ensureCapacity(BUFFER_SIZE);
        gcName.ensureCapacity(BUFFER_SIZE);
        gcAction.ensureCapacity(BUFFER_SIZE);
        gcCause.ensureCapacity(BUFFER_SIZE);

        columns = new LinkedHashMap<>();
        columns.put("Id", id);
        columns.put("Start", start);
        columns.put("End", end);
        columns.put("GcName", gcName);
        columns.put("GcAction", gcAction);
        columns.put("GcCause", gcCause);
        ix = 0;
    }

    public boolean isFull() {
        return ix >= BUFFER_SIZE;
    }

    public void add(GarbageCollectionNotificationInfo gcNotification) {
        final GcInfo gcInfo = gcNotification.getGcInfo();
        // todo ensure capacity
        id.set(ix, gcInfo.getId());
        start.set(ix, DateTimeUtils.millisToNanos(vmStartMillis + gcInfo.getStartTime()));
        end.set(ix, DateTimeUtils.millisToNanos(vmStartMillis + gcInfo.getEndTime()));
        gcName.set(ix, gcNotification.getGcName());
        gcAction.set(ix, gcNotification.getGcAction());
        gcCause.set(ix, gcNotification.getGcCause());
        ++ix;
    }

    public Table table() {
        return new QueryTable(definition(), RowSetFactory.flat(ix).toTracking(), columns);
    }

    public void reset() {
        ix = 0;
    }
}
