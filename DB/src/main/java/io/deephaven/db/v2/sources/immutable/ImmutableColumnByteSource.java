package io.deephaven.db.v2.sources.immutable;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;
import io.deephaven.qst.table.column.Column;

public final class ImmutableColumnByteSource extends ImmutableColumnSource<Byte> implements ImmutableColumnSourceGetDefaults.ForByte {

    public ImmutableColumnByteSource(Column<Byte> column) {
        super(column, byte.class);
    }

    @Override
    public final byte getByte(long index) {
        // We store the values in boxed form for Column<T>,
        // so no reason to try and be more efficient here
        Byte boxed = get(index);
        return boxed == null ? NULL_BYTE : boxed;
    }
}
