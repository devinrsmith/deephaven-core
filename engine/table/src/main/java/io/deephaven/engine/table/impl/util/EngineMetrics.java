/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.clock.Clock;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.tablelogger.EngineTableLoggers;
import io.deephaven.engine.tablelogger.ProcessInfoLogLogger;
import io.deephaven.engine.tablelogger.ProcessMetricsLogLogger;
import io.deephaven.engine.tablelogger.QueryOperationPerformanceLogLogger;
import io.deephaven.engine.tablelogger.QueryPerformanceLogLogger;
import io.deephaven.engine.tablelogger.impl.memory.MemoryTableLogger;
import io.deephaven.engine.tablelogger.impl.memory.QueryPerformanceNuggetConfig;
import io.deephaven.io.logger.Logger;
import io.deephaven.stats.StatsIntradayLogger;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.process.ProcessInfo;
import io.deephaven.process.ProcessInfoConfig;
import io.deephaven.process.ProcessInfoStoreDBImpl;
import io.deephaven.process.StatsIntradayLoggerDBImpl;
import io.deephaven.stats.Driver;
import io.deephaven.stream.blink.BlinkTableMapper;
import io.deephaven.stream.blink.Producer;

import java.io.IOException;

public class EngineMetrics {
    private static final boolean STATS_LOGGING_ENABLED = Configuration.getInstance().getBooleanWithDefault(
            "statsLoggingEnabled", true);
    private volatile static ProcessInfo processInfo;
    private volatile static EngineMetrics INSTANCE;

    public static ProcessInfo getProcessInfo() {
        if (processInfo == null) {
            synchronized (EngineMetrics.class) {
                if (processInfo == null) {
                    try {
                        processInfo = ProcessInfoConfig.createForCurrentProcess(Configuration.getInstance());
                    } catch (IOException e) {
                        throw new IllegalStateException("Failed to create process info.", e);
                    }
                }
            }
        }
        return processInfo;
    }

    public static EngineMetrics getInstance() {
        if (INSTANCE == null) {
            synchronized (EngineMetrics.class) {
                if (INSTANCE == null) {
                    INSTANCE = new EngineMetrics();
                }
            }
        }
        return INSTANCE;
    }

    private final QueryPerformanceLogLogger qplLogger;
    private final BlinkTableMapper<QueryPerformanceNugget> qplBlink;
    private final QueryOperationPerformanceLogLogger qoplLogger;
    private final ProcessInfoLogLogger processInfoLogger;
    private final ProcessMetricsLogLogger processMetricsLogger;
    private final StatsIntradayLogger statsLogger;

    private EngineMetrics() {
        EngineTableLoggers.Factory tableLoggerFactory = EngineTableLoggers.get();
        final Logger log = LoggerFactory.getLogger(EngineMetrics.class);
        ProcessInfo pInfo = null;
        ProcessInfoLogLogger pInfoLogger = null;
        try {
            pInfo = getProcessInfo();
            pInfoLogger = tableLoggerFactory.processInfoLogLogger();
            new ProcessInfoStoreDBImpl(pInfoLogger).put(pInfo);
        } catch (IOException e) {
            log.fatal().append("Failed to configure process info: ").append(e.toString()).endl();
        }
        processInfoLogger = pInfoLogger;
        qplLogger = tableLoggerFactory.queryPerformanceLogLogger();
        qoplLogger = tableLoggerFactory.queryOperationPerformanceLogLogger();
        if (STATS_LOGGING_ENABLED) {
            processMetricsLogger = tableLoggerFactory.processMetricsLogLogger();
            statsLogger = new StatsIntradayLoggerDBImpl(pInfo.getId(), processMetricsLogger);
        } else {
            processMetricsLogger = null;
            statsLogger = null;
        }

        // todo: other context
        qplBlink = BlinkTableMapper.create(
                QueryPerformanceNuggetConfig.config("todo", 1024, ExecutionContext.getContext().getUpdateGraph()));
    }

    public QueryTable getQplLoggerQueryTable() {
        return MemoryTableLogger.maybeGetQueryTable(qplLogger);
    }

    public Table qplBlinkTable() {
        return qplBlink.table();
    }

    public QueryTable getQoplLoggerQueryTable() {
        return MemoryTableLogger.maybeGetQueryTable(qoplLogger);
    }

    public QueryPerformanceLogLogger getQplLogger() {
        return qplLogger;
    }

    public Producer<QueryPerformanceNugget> blinkProducer() {
        return qplBlink.producer();
    }

    public QueryOperationPerformanceLogLogger getQoplLogger() {
        return qoplLogger;
    }

    public QueryTable getProcessInfoQueryTable() {
        return MemoryTableLogger.maybeGetQueryTable(processInfoLogger);
    }

    public QueryTable getProcessMetricsQueryTable() {
        if (processMetricsLogger != null) {
            return MemoryTableLogger.maybeGetQueryTable(processMetricsLogger);
        }
        return null;
    }

    private StatsIntradayLogger getStatsLogger() {
        return statsLogger;
    }

    public static boolean maybeStartStatsCollection() {
        if (!EngineMetrics.STATS_LOGGING_ENABLED) {
            return false;
        }
        final boolean fdStatsLoggingEnabled = Configuration.getInstance().getBooleanWithDefault(
                "fdStatsLoggingEnabled", false);
        Driver.start(Clock.system(), EngineMetrics.getInstance().getStatsLogger(), fdStatsLoggingEnabled);
        return true;
    }
}
