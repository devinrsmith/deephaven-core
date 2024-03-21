/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

@BuildableStyle
@Immutable
public abstract class JsonFromTable {
    public static Builder builder() {
        return ImmutableJsonFromTable.builder();
    }

    public abstract Table table();

    public abstract Optional<String> columnName();

    public abstract ValueOptions options();

    @Default
    public int chunkSize() {
        return 1024; // todo
    }

    public final Table execute() {
        final ColumnDefinition<?> columnDefinition = columnName().isPresent()
                ? table().getDefinition().getColumn(columnName().get())
                : table().getDefinition().getColumns().get(0);
        return tableToTable(columnDefinition).execute();
    }

    public interface Builder {
        Builder table(Table table);

        Builder columnName(String columnName);

        Builder options(ValueOptions options);

        Builder chunkSize(int chunkSize);

        JsonFromTable build();

        default Table execute() {
            return build().execute();
        }
    }

    private <T> TableToTableOptions<T> tableToTable(ColumnDefinition<T> columnDefinition) {
        final Type<T> type = columnDefinition.type();
        final TableToTableOptions.Builder<T> builder = TableToTableOptions.<T>builder()
                .table(table())
                .columnType(type)
                .processor(options().named(type))
                .chunkSize(chunkSize());
        columnName().ifPresent(builder::columnName);
        return builder.build();
    }
}
