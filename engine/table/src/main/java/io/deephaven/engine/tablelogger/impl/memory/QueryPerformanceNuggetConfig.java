package io.deephaven.engine.tablelogger.impl.memory;

import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.util.EngineMetrics;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.stream.blink.BlinkTableMapperConfig;
import io.deephaven.util.QueryConstants;

import java.time.Instant;

public class QueryPerformanceNuggetConfig {

    public static BlinkTableMapperConfig<QueryPerformanceNugget> config(
            String name, int chunkSize, UpdateSourceRegistrar usr) {
        return BlinkTableMapperConfig.<QueryPerformanceNugget>builder()
                .name(name)
                .chunkSize(chunkSize)
                .updateSourceRegistrar(usr)
                // row flags?
                // OperationNumber
                .putString("ProcessUniqueId", QueryPerformanceNuggetConfig::processUniqueId)
                .putInt("EvaluationNumber", QueryPerformanceNugget::getEvaluationNumber)
                .putInt("Depth", QueryPerformanceNugget::getDepth)
                .putString("Description", QueryPerformanceNugget::getName)
                .putString("CallerLine", QueryPerformanceNugget::getCallerLine)
                .putBoolean("IsTopLevel", QueryPerformanceNugget::isTopLevel)
                .putBoolean("IsCompilation", QueryPerformanceNuggetConfig::isCompilation)
                .putInstant("StartTime", QueryPerformanceNuggetConfig::startTime)
                .putInstant("EndTime", QueryPerformanceNuggetConfig::endTime)
                .putLong("DurationNanos", QueryPerformanceNuggetConfig::durationNanos)
                .putLong("CpuNanos", QueryPerformanceNugget::getCpuNanos)
                .putLong("UserCpuNanos", QueryPerformanceNugget::getUserCpuNanos)
                .putLong("FreeMemoryChange", QueryPerformanceNugget::getDiffFreeMemory)
                .putLong("TotalMemoryChange", QueryPerformanceNugget::getDiffTotalMemory)
                .putLong("Collections", QueryPerformanceNugget::getDiffCollections)
                .putLong("CollectionTimeNanos", QueryPerformanceNugget::getDiffCollectionTimeNanos)
                .putLong("AllocatedBytes", QueryPerformanceNugget::getAllocatedBytes)
                .putLong("PoolAllocatedBytes", QueryPerformanceNugget::getPoolAllocatedBytes)
                .putLong("InputSizeLong", QueryPerformanceNugget::getInputSize)
                .putBoolean("WasInterrupted", QueryPerformanceNugget::wasInterrupted)
                // this.IsReplayer.setBoolean(queryProcessingResults.isReplayer());
                // this.Exception.set(queryProcessingResults.getException());
                .build();
    }

    private static String processUniqueId(QueryPerformanceNugget n) {
        // todo
        return EngineMetrics.getProcessInfo().getId().value();
    }

    private static boolean isCompilation(QueryPerformanceNugget n) {
        return n.getName().startsWith("Compile:");
    }

    private static Instant startTime(QueryPerformanceNugget n) {
        return Instant.ofEpochMilli(n.getStartClockTime());
    }

    private static Instant endTime(QueryPerformanceNugget n) {
        return n.getTotalTimeNanos() == null ? null : startTime(n).plusNanos(n.getTotalTimeNanos());
    }

    private static long durationNanos(QueryPerformanceNugget n) {
        return n.getTotalTimeNanos() == null ? QueryConstants.NULL_LONG : n.getTotalTimeNanos();
    }
}
