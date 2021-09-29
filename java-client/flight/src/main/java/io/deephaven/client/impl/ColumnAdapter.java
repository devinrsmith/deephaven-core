package io.deephaven.client.impl;

import io.deephaven.qst.column.Column;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;

public class ColumnAdapter {

    public static FieldVector of(Column<?> column, RootAllocator allocator) {
        return ArrayAdapter.of(column.name(), column.array(), allocator);
    }
}
