package io.deephaven.qst;

import io.deephaven.qst.ImmutableNewTable.Builder;
import java.util.stream.Stream;

public abstract class NewTableBuildable {

    protected abstract Stream<Column<?>> columns();

    public final NewTable build() {
        Builder builder = ImmutableNewTable.builder();
        columns().forEachOrdered(builder::addColumns);
        return builder.build();
    }
}
