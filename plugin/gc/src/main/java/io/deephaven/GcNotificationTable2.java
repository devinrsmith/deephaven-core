package io.deephaven;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamToTableAdapter;
import io.deephaven.time.DateTimeUtils;

import java.lang.management.ManagementFactory;
import java.util.Objects;

final class GcNotificationTable2 {

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofLong("Id"),
            ColumnDefinition.ofTime("Start"),
            ColumnDefinition.ofTime("End"),
            ColumnDefinition.ofString("GcName"),
            ColumnDefinition.ofString("GcAction"),
            ColumnDefinition.ofString("GcCause"));
    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public static TableDefinition definition() {
        return DEFINITION;
    }

    public static GcNotificationTable2 of(StreamConsumer consumer) {
        return new GcNotificationTable2(consumer, ManagementFactory.getRuntimeMXBean().getStartTime());
    }

    private final StreamConsumer consumer;
    private final long vmStartMillis;
    private WritableChunk<Values>[] chunks;

    private GcNotificationTable2(StreamConsumer consumer, long vmStartMillis) {
        this.consumer = Objects.requireNonNull(consumer);
        this.vmStartMillis = vmStartMillis;
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
        if (chunks[0].size() == CHUNK_SIZE) {
            flush();
        }
    }

    public synchronized void flush() {
        if (chunks[0].size() == 0) {
            return;
        }
        consumer.accept(chunks);
        // noinspection unchecked
        chunks = StreamToTableAdapter.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
    }

    public void acceptFailure(Throwable e) {
        consumer.acceptFailure(e);
    }
}
