package io.deephaven.db.v2.sources.immutable;

import static io.deephaven.util.QueryConstants.NULL_INT;

import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;
import io.deephaven.qst.Column;

public final class ImmutableColumnIntegerSource extends ImmutableColumnSource<Integer> implements ImmutableColumnSourceGetDefaults.ForInt {

    public ImmutableColumnIntegerSource(Column<Integer> column) {
        super(column, int.class);
    }

    @Override
    public final int getInt(long index) {
        // We store the values in boxed form for Column<T>,
        // so no reason to try and be more efficient here
        Integer boxed = get(index);
        return boxed == null ? NULL_INT : boxed;
    }
}
