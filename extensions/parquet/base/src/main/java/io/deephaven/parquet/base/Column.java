package io.deephaven.parquet.base;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.immutables.value.Value;

import java.util.Arrays;

@Value.Immutable
public abstract class Column {

    // TODO: really should do a better job at capturing struct around Type

    static Column of(Schema schema, int index, ColumnDescriptor columnDescriptor) {
        return null;
    }

    @Value.Parameter
    abstract Schema schema();

    @Value.Parameter
    abstract int index();

    @Value.Parameter
    public abstract ColumnDescriptor columnDescriptor();

    @Value.Derived
    public ColumnPath path() {
        return ColumnPath.get(columnDescriptor().getPath());
    }

    public final boolean hasPath(String... path) {
        return Arrays.equals(columnDescriptor().getPath(), path);
    }

    public final boolean hasPath(ColumnPath path) {
        return hasPath(path.toArray());
    }
}
