package io.deephaven.metadata;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeClassBase;
import io.deephaven.qst.column.Column;

import java.io.OutputStream;

@AutoService(ObjectType.class)
public final class StringArrayObjectType extends ObjectTypeClassBase<String[]> {

    public StringArrayObjectType() {
        super(String[].class);
    }

    @Override
    public void writeToImpl(Exporter exporter, String[] value, OutputStream out) {
        exporter.reference(table(value), false, true).orElseThrow();
    }

    private static Table table(String[] value) {
        return InMemoryTable.from(Column.of("Value", value).toTable());
    }
}
