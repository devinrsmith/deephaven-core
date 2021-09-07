package io.deephaven.extensions;

import io.deephaven.appmode.Application;
import io.deephaven.appmode.Field;
import io.deephaven.appmode.Fields;
import io.deephaven.appmode.StandardField;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;

public class CustomApplication implements Application.Factory {

    @Override
    public Application create() {
        Field<Table> hello = StandardField.of("hello", TableTools.emptyTable(42).view("I=i"),
                "A table with one column 'I' and 42 rows, 0-41.");
        Field<Table> world = StandardField.of("world", TableTools.timeTable("00:00:01"));
        return Application.builder()
                .id(CustomApplication.class.getName())
                .name("Custom Application")
                .fields(Fields.of(hello, world))
                .build();
    }
}
