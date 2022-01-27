package io.deephaven.metadata;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeClassBase;
import io.deephaven.qst.column.Column;

import java.io.OutputStream;

@AutoService(ObjectType.class)
public final class LongObjectType extends ObjectTypeClassBase<Long> {

    public LongObjectType() {
        super(Long.class);
    }

    @Override
    public void writeToImpl(Exporter exporter, Long value, OutputStream out) {
        exporter.reference(table(value), false, true).orElseThrow();
    }

    private static Table table(Long x) {
        return InMemoryTable.from(Column.of("Value", x).toTable());
    }
}
