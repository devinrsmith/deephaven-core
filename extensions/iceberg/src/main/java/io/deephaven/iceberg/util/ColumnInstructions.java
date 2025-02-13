package io.deephaven.iceberg.util;

import io.deephaven.annotations.SimpleStyle;
import org.apache.iceberg.types.Types.NestedField;
import org.immutables.value.Value;

@Value.Immutable
@SimpleStyle
public abstract class ColumnInstructions {

    public static ColumnInstructions of(FieldPath path) {
        return ImmutableColumnInstructions.of(path);
    }

    /**
     * The path to the Iceberg {@link NestedField} associated with this column.
     */
    @Value.Parameter
    abstract FieldPath path();

    // Note: very likely there will be additions here to support future additions; codecs, conversions, etc.
}
