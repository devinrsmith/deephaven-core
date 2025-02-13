package io.deephaven.iceberg.internal;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.List;

public abstract class ColumnInstructionsBase implements Mapping.ColumnInstructions {

    abstract int[] path();

    public final List<Types.NestedField> fieldPath(Schema schema) {
        return SchemaHelper.fieldPath(schema, path());
    }
}
