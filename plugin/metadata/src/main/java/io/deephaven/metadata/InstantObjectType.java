package io.deephaven.metadata;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeClassBase;
import io.deephaven.qst.column.Column;

import java.io.OutputStream;
import java.time.Instant;

@AutoService(ObjectType.class)
public final class InstantObjectType extends ObjectTypeClassBase<Instant> {

    public InstantObjectType() {
        super(Instant.class);
    }

    @Override
    public void writeToImpl(Exporter exporter, Instant timestamp, OutputStream out) {
        exporter.reference(table(timestamp), false, true).orElseThrow();
    }

    static Table table(Instant timestamp) {
        return InMemoryTable.from(Column.of("Timestamp", timestamp).toTable());
    }
}
