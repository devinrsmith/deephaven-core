package io.deephaven.db.v2.sources.immutable;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;
import io.deephaven.qst.table.column.Column;

public final class ImmutableColumnShortSource extends ImmutableColumnSource<Short> implements ImmutableColumnSourceGetDefaults.ForShort {

    public ImmutableColumnShortSource(Column<Short> column) {
        super(column, short.class);
    }

    @Override
    public final short getShort(long index) {
        // We store the values in boxed form for Column<T>,
        // so no reason to try and be more efficient here
        Short boxed = get(index);
        return boxed == null ? NULL_SHORT : boxed;
    }
}
