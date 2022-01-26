package io.deephaven.metadata;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.Table;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeClassBase;
import io.deephaven.time.DateTime;

import java.io.OutputStream;
import java.time.Instant;

@AutoService(ObjectType.class)
public final class DateTimeType extends ObjectTypeClassBase<DateTime> {

    public DateTimeType() {
        super(DateTime.class.getName(), DateTime.class);
    }

    @Override
    public void writeToImpl(Exporter exporter, DateTime timestamp, OutputStream out) {
        exporter.reference(table(timestamp), false, true).orElseThrow();
    }

    private static Table table(DateTime timestamp) {
        return InstantObjectType.table(timestamp.getInstant()); // todo: more efficient way
    }
}
