package io.deephaven.db.v2.sources.immutable;

import static io.deephaven.util.QueryConstants.NULL_LONG;

import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;
import io.deephaven.qst.Column;

public final class ImmutableColumnLongSource extends ImmutableColumnSource<Long> implements ImmutableColumnSourceGetDefaults.ForLong {

    public ImmutableColumnLongSource(Column<Long> column) {
        super(column, long.class);
    }

    @Override
    public final long getLong(long index) {
        // We store the values in boxed form for Column<T>,
        // so no reason to try and be more efficient here
        Long boxed = get(index);
        return boxed == null ? NULL_LONG : boxed;
    }
}
