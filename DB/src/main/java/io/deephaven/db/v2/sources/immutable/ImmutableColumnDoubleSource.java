package io.deephaven.db.v2.sources.immutable;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;
import io.deephaven.qst.column.Column;

public final class ImmutableColumnDoubleSource extends ImmutableColumnSource<Double> implements ImmutableColumnSourceGetDefaults.ForDouble {

    public ImmutableColumnDoubleSource(Column<Double> column) {
        super(column, double.class);
    }

    @Override
    public final double getDouble(long index) {
        // We store the values in boxed form for Column<T>,
        // so no reason to try and be more efficient here
        Double boxed = get(index);
        return boxed == null ? NULL_DOUBLE : boxed;
    }
}
