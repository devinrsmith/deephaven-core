package io.deephaven.db.v2.sources.immutable;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;
import io.deephaven.qst.Column;

public final class ImmutableColumnCharSource extends ImmutableColumnSource<Character> implements ImmutableColumnSourceGetDefaults.ForChar {

    public ImmutableColumnCharSource(Column<Character> column) {
        super(column, char.class);
    }

    @Override
    public final char getChar(long index) {
        // We store the values in boxed form for Column<T>,
        // so no reason to try and be more efficient here
        Character boxed = get(index);
        return boxed == null ? NULL_CHAR : boxed;
    }
}
