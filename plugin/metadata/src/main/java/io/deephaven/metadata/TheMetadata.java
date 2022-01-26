package io.deephaven.metadata;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.plugin.app.App;
import io.deephaven.plugin.app.App.Consumer;
import io.deephaven.qst.column.Column;

import java.time.Instant;
import java.util.Objects;


public final class TheMetadata implements App.State {

    private final String version;
    private final Instant startTime;

    public TheMetadata(String version, Instant startTime) {
        this.version = Objects.requireNonNull(version);
        this.startTime = Objects.requireNonNull(startTime);
    }

    @Override
    public void insertInto(Consumer consumer) {
        consumer.set("version", versionTable(), "The server version. [Version: STRING]");
        consumer.set("startTime", startTimeTable(), "The server start time. [Timestamp: TIMESTAMP]");
    }

    public String version() {
        return version;
    }

    public Table versionTable() {
        return InMemoryTable.from(Column.of("Version", version()).toTable());
    }

    public Instant startTime() {
        return startTime;
    }

    public Table startTimeTable() {
        return InMemoryTable.from(Column.of("Timestamp", startTime()).toTable());
    }
}
