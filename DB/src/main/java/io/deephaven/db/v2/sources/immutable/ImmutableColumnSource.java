package io.deephaven.db.v2.sources.immutable;

import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.qst.Column;
import java.util.Objects;

public abstract class ImmutableColumnSource<T> extends AbstractColumnSource<T> {

    private final Column<T> column;

    public ImmutableColumnSource(Column<T> column, Class<T> clazz) {
        super(clazz);
        this.column = Objects.requireNonNull(column);
    }

    @Override
    public final T get(long index) {
        if (index < 0 || index >= column.size()) {
            return null;
        }
        return column.values().get((int)index);
    }

    @Override
    public final boolean isImmutable() {
        return true;
    }
}
