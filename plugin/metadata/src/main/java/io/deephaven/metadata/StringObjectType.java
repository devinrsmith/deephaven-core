package io.deephaven.metadata;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeClassBase;
import io.deephaven.qst.column.Column;

import java.io.OutputStream;

@AutoService(ObjectType.class)
public final class StringObjectType extends ObjectTypeClassBase<String> {

    public StringObjectType() {
        super(String.class.getName(), String.class);
    }

    @Override
    public void writeToImpl(Exporter exporter, String string, OutputStream out) {
        exporter.reference(table(string), false, true).orElseThrow();
    }

    private static Table table(String string) {
        return InMemoryTable.from(Column.of("String", string).toTable());
    }
}
