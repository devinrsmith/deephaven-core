package io.deephaven.db.v2.sources.immutable;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;
import io.deephaven.qst.column.Column;

public final class ImmutableColumnFloatSource extends ImmutableColumnSource<Float> implements ImmutableColumnSourceGetDefaults.ForFloat {

    public ImmutableColumnFloatSource(Column<Float> column) {
        super(column, float.class);
    }

    @Override
    public final float getFloat(long index) {
        // We store the values in boxed form for Column<T>,
        // so no reason to try and be more efficient here
        Float boxed = get(index);
        return boxed == null ? NULL_FLOAT : boxed;
    }
}
