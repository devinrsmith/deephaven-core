package io.deephaven.parquet.format;

import org.apache.parquet.schema.GroupType;
import org.immutables.value.Value;

@Value.Immutable
public abstract class GroupField {

    @Value.Parameter
    abstract GroupType type();
}
