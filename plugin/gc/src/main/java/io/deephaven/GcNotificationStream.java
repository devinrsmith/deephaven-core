package io.deephaven;

import com.sun.management.GarbageCollectionNotificationInfo;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.stream.TableToStream;
import io.deephaven.util.SafeCloseable;

import java.util.Objects;

final class GcNotificationStream implements Runnable, SafeCloseable {

    public static GcNotificationStream of(String name, UpdateSourceRegistrar updateSourceRegistrar,
            Runnable shutdownCallback) {
        final SetableRunnable flush = new SetableRunnable();
        final TableToStream tableToStream = TableToStream.of(name, GcNotificationTable.definition(),
                updateSourceRegistrar, flush, shutdownCallback);
        final GcNotificationStream stream = new GcNotificationStream(tableToStream, new GcNotificationTable());
        flush.set(stream::flush);
        return stream;

    }

    private final TableToStream tableToStream;
    private final GcNotificationTable table;

    private GcNotificationStream(TableToStream tableToStream, GcNotificationTable table) {
        this.tableToStream = Objects.requireNonNull(tableToStream);
        this.table = Objects.requireNonNull(table);
    }

    public Table table() {
        return tableToStream.table();
    }

    public synchronized void add(GarbageCollectionNotificationInfo gcNotification) {
        table.add(gcNotification);
        if (table.isFull()) {
            flush();
        }
    }

    public synchronized void flush() {
        try {
            tableToStream.consumer(GcNotificationTable.BUFFER_SIZE, false).add(table.table());
        } finally {
            table.reset();
        }
    }

    @Override
    public void run() {
        tableToStream.run();
    }

    @Override
    public void close() {
        tableToStream.close();
    }
}
