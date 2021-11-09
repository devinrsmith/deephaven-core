package com.devinrsmith;

import io.deephaven.db.tables.Table;
import io.deephaven.qst.array.IntArray;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableSpec;

import java.util.Collection;

public class FooBarLogic {
    public static Table fooBar(Collection<Integer> fooGroup, Collection<Integer> barGroup) {
        final TableSpec foo = NewTable.of(Column.of("Foo", IntArray.of(fooGroup)));
        final TableSpec bar = NewTable.of(Column.of("Bar", IntArray.of(barGroup)));
        final TableSpec results = foo
                .join(bar, "")
                .updateView("Add=Foo+Bar", "Mult=Foo*Bar");
        return Table.of(results);
    }
}
