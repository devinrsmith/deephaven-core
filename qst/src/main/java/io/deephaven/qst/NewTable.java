package io.deephaven.qst;

import io.deephaven.qst.ImmutableTableHeader.Builder;
import java.util.Iterator;
import java.util.List;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class NewTable extends TableBase {

    static NewTable of(Column<?>... columns) {
        return ImmutableNewTable.builder().addColumns(columns).build();
    }

    static NewTable of(Iterable<Column<?>> columns) {
        return ImmutableNewTable.builder().addAllColumns(columns).build();
    }

    static <A> ColumnHeader<A> header(String name, Class<A> clazz) {
        return ColumnHeader.of(name, clazz);
    }

    public abstract List<Column<?>> columns();

    public final TableHeader header() {
        Builder builder = ImmutableTableHeader.builder();
        for (Column<?> column : columns()) {
          builder.addHeaders(column.header());
        }
        return builder.build();
    }

    public final NewTable with(Column<?> column) {
        return ImmutableNewTable.builder()
            .addAllColumns(columns())
            .addColumns(column)
            .build();
    }

    @Check
    final void checkColumnsSizes() {
        if (columns()
            .stream()
            .map(Column::values)
            .mapToInt(List::size)
            .distinct()
            .limit(2)
            .count() > 1) {
            throw new IllegalArgumentException("All columns must be the same size");
        }
    }

    @Check
    final void checkValidHeader() {
        header(); // ensure we can build the header
    }
}
