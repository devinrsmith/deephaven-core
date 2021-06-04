package io.deephaven.db.v2.sources.immutable;

import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;
import io.deephaven.qst.table.column.Column;

public final class ImmutableColumnBooleanSource extends ImmutableColumnSource<Boolean> implements ImmutableColumnSourceGetDefaults.ForBoolean {

    public ImmutableColumnBooleanSource(Column<Boolean> column) {
        super(column, boolean.class);
    }
}
