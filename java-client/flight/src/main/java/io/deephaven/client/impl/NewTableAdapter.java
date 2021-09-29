package io.deephaven.client.impl;

import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;
import java.util.List;

public class NewTableAdapter {
    public static VectorSchemaRoot of(NewTable table, RootAllocator allocator) {
        final List<FieldVector> fieldVectors = new ArrayList<>(table.numColumns());
        for (Column<?> column : table) {
            fieldVectors.add(ColumnAdapter.of(column, allocator));
        }
        return new VectorSchemaRoot(fieldVectors);
    }
}
