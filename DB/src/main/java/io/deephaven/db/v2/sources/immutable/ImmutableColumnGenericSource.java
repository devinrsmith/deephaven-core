package io.deephaven.db.v2.sources.immutable;

import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;
import io.deephaven.qst.Column;

public final class ImmutableColumnGenericSource<T> extends ImmutableColumnSource<T> implements ImmutableColumnSourceGetDefaults.ForObject<T> {

    public ImmutableColumnGenericSource(Column<T> column, Class<T> clazz) {
        super(column, clazz); // todo
    }
}
