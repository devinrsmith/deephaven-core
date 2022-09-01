package io.deephaven;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.util.PercentileOutput;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamToTableAdapter;
import io.deephaven.time.DateTimeUtils;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.Arrays;
import java.util.Objects;

final class GcNotificationConsumer {

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofLong("Id"),
            ColumnDefinition.ofTime("Start"),
            ColumnDefinition.ofTime("End"),
            ColumnDefinition.ofString("GcName"),
            ColumnDefinition.ofString("GcAction"),
            ColumnDefinition.ofString("GcCause"),
            ColumnDefinition.ofLong("Reclaimed"));
    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public static TableDefinition definition() {
        return DEFINITION;
    }

    public static Table stats(Table notificationInfo) {
        return notificationInfo
                .updateView("Duration=(End-Start)/1000000000")
                .aggBy(Arrays.asList(
                                Aggregation.AggCount("Count"),
                                Aggregation.AggLast(
                                        "LastId=Id",
                                        "LastStart=Start",
                                        "LastEnd=End",
                                        "LastReclaimed=Reclaimed"),
                                Aggregation.AggSum("DurationTotal=Duration", "ReclaimedTotal=Reclaimed"),
                                Aggregation.AggMax("DurationMax=Duration", "ReclaimedMax=Reclaimed"),
                                Aggregation.AggAvg("DurationAvg=Duration", "ReclaimedAvg=Reclaimed"),
                                Aggregation.AggApproxPct("Duration",
                                        PercentileOutput.of(0.5, "DurationP_50"),
                                        PercentileOutput.of(0.9, "DurationP_90"),
                                        PercentileOutput.of(0.95, "DurationP_95"),
                                        PercentileOutput.of(0.99, "DurationP_99")),
                                Aggregation.AggApproxPct("Reclaimed",
                                        PercentileOutput.of(0.5, "ReclaimedP_50"),
                                        PercentileOutput.of(0.9, "ReclaimedP_90"),
                                        PercentileOutput.of(0.95, "ReclaimedP_95"),
                                        PercentileOutput.of(0.99, "ReclaimedP_99"))),
                        "GcName", "GcAction", "GcCause");
    }

    private final StreamConsumer consumer;
    private final long vmStartMillis;
    private WritableChunk<Values>[] chunks;

    GcNotificationConsumer(StreamConsumer consumer) {
        this.consumer = Objects.requireNonNull(consumer);
        this.vmStartMillis = ManagementFactory.getRuntimeMXBean().getStartTime();
        // noinspection unchecked
        chunks = StreamToTableAdapter.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
    }

    public synchronized void add(GarbageCollectionNotificationInfo gcNotification) {
        final GcInfo gcInfo = gcNotification.getGcInfo();
        chunks[0].asWritableLongChunk().add(gcInfo.getId());
        chunks[1].asWritableLongChunk().add(DateTimeUtils.millisToNanos(vmStartMillis + gcInfo.getStartTime()));
        chunks[2].asWritableLongChunk().add(DateTimeUtils.millisToNanos(vmStartMillis + gcInfo.getEndTime()));
        chunks[3].<String>asWritableObjectChunk().add(gcNotification.getGcName().intern());
        chunks[4].<String>asWritableObjectChunk().add(gcNotification.getGcAction().intern());
        chunks[5].<String>asWritableObjectChunk().add(gcNotification.getGcCause().intern());

        // This is a bit of a de-normalization - arguably, it could be computed by joining against the "pools" table.
        // But this is a very useful summary value, and easy for use to provide here for more convenience.
        final long usedBefore = gcNotification.getGcInfo().getMemoryUsageBeforeGc().values().stream().mapToLong(MemoryUsage::getUsed).sum();
        final long usedAfter = gcNotification.getGcInfo().getMemoryUsageAfterGc().values().stream().mapToLong(MemoryUsage::getUsed).sum();
        // Note: reclaimed *can* be negative
        final long reclaimed = usedBefore - usedAfter;
        chunks[6].asWritableLongChunk().add(reclaimed);

        if (chunks[0].size() == CHUNK_SIZE) {
            flushInternal();
        }
    }

    public synchronized void flush() {
        if (chunks[0].size() == 0) {
            return;
        }
        flushInternal();
    }

    private void flushInternal() {
        consumer.accept(chunks);
        // noinspection unchecked
        chunks = StreamToTableAdapter.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
    }

    public void acceptFailure(Throwable e) {
        consumer.acceptFailure(e);
    }
}
