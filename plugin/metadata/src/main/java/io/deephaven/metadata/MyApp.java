package io.deephaven.metadata;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.plugin.app.App;
import io.deephaven.plugin.app.AppBase;
import io.deephaven.qst.column.Column;

import java.time.Instant;

@AutoService(App.class)
public final class MyApp extends AppBase {

    private final String version = "todo";
    private final Instant startTime = Instant.now();

    public MyApp() {
        super(MyApp.class.getName(), MyApp.class.getSimpleName());
    }

    @Override
    public void insertInto(Consumer consumer) {
        consumer.set("version", version);
        consumer.set("startTime", startTime);
        consumer.set("versionTable", versionTable());
        consumer.set("startTimeTable", startTimeTable());
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
