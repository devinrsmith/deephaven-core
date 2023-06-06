package io.deephaven.app;

import com.sun.management.GarbageCollectionNotificationInfo;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.util.PercentileOutput;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.stream.blink.BlinkTableMapperConfig;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.time.Instant;
import java.util.Arrays;

final class GcNotificationPublisher {

    private static final long VM_START_TIME_EPOCH_MILLIS = ManagementFactory.getRuntimeMXBean().getStartTime();

    public static BlinkTableMapperConfig<GarbageCollectionNotificationInfo> create(
            String name, int chunkSize, UpdateSourceRegistrar usr) {
        return BlinkTableMapperConfig.<GarbageCollectionNotificationInfo>builder()
                .name(name)
                .chunkSize(chunkSize)
                .updateSourceRegistrar(usr)
                .putLong("Id", GcNotificationPublisher::id)
                .putInstant("Start", GcNotificationPublisher::start)
                .putInstant("End", GcNotificationPublisher::end)
                .putString("GcName", GarbageCollectionNotificationInfo::getGcName)
                .putString("GcAction", GarbageCollectionNotificationInfo::getGcAction)
                .putString("GcCause", GarbageCollectionNotificationInfo::getGcCause)
                .putLong("Reclaimed", GcNotificationPublisher::reclaimed)
                .build();
    }

    public static Table stats(Table notificationInfo) {
        // Would be great to have a aggBy Exponential Decayed Sum so we could have "GC rate over the last 1, 5, 15 min"
        // Could further extend with updateBy Exponential Decayed Sum and graphs of rates.
        return notificationInfo
                .updateView("Duration=(End-Start)/1000000000")
                .aggBy(Arrays.asList(
                        Aggregation.AggCount("Count"),
                        Aggregation.AggSum("DurationTotal=Duration", "ReclaimedTotal=Reclaimed"),
                        Aggregation.AggMax("DurationMax=Duration"),
                        Aggregation.AggAvg("DurationAvg=Duration", "ReclaimedAvg=Reclaimed"),
                        Aggregation.AggApproxPct("Reclaimed",
                                PercentileOutput.of(0.5, "ReclaimedP_50")),
                        Aggregation.AggApproxPct("Duration",
                                PercentileOutput.of(0.5, "DurationP_50"),
                                PercentileOutput.of(0.9, "DurationP_90"),
                                PercentileOutput.of(0.95, "DurationP_95"),
                                PercentileOutput.of(0.99, "DurationP_99")),
                        Aggregation.AggLast(
                                "LastId=Id",
                                "LastStart=Start",
                                "LastEnd=End",
                                "LastReclaimed=Reclaimed")),
                        "GcName", "GcAction", "GcCause");
    }

    private static long id(GarbageCollectionNotificationInfo gcNotification) {
        return gcNotification.getGcInfo().getId();
    }

    private static Instant start(GarbageCollectionNotificationInfo gcNotification) {
        return Instant.ofEpochMilli(VM_START_TIME_EPOCH_MILLIS + gcNotification.getGcInfo().getStartTime());
    }

    private static Instant end(GarbageCollectionNotificationInfo gcNotification) {
        return Instant.ofEpochMilli(VM_START_TIME_EPOCH_MILLIS + gcNotification.getGcInfo().getEndTime());
    }

    private static long reclaimed(GarbageCollectionNotificationInfo gcNotification) {
        // This is a bit of a de-normalization - arguably, it could be computed by joining against the "pools" table.
        // But this is a very useful summary value, and easy for use to provide here for more convenience.
        final long usedBefore = gcNotification.getGcInfo().getMemoryUsageBeforeGc().values().stream()
                .mapToLong(MemoryUsage::getUsed).sum();
        final long usedAfter = gcNotification.getGcInfo().getMemoryUsageAfterGc().values().stream()
                .mapToLong(MemoryUsage::getUsed).sum();
        // Note: reclaimed *can* be negative
        return usedBefore - usedAfter;
    }
}
