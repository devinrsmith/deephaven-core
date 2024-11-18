package io.deephaven.parquet.format;

import org.apache.parquet.schema.PrimitiveType;
import org.immutables.value.Value;

@Value.Immutable
public abstract class PrimitiveField {

    @Value.Parameter
    abstract PrimitiveType type();
}
