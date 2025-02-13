package io.deephaven.iceberg.internal;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value;

@Value.Immutable
@SimpleStyle
public abstract class FieldIdPath extends ColumnInstructionsBase {

    public static FieldIdPath of(int[] path) {
        return ImmutableFieldIdPath.of(path);
    }

    @Value.Parameter
    public abstract int[] path();
}
