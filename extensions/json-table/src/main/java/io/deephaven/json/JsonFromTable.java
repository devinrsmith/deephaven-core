//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.Table;
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
        return tableToTable().execute();
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

    private  TableToTableOptions tableToTable() {
        final TableToTableOptions.Builder builder = TableToTableOptions.builder()
                .table(table())
                .processor(options())
                .chunkSize(chunkSize());
        columnName().ifPresent(builder::columnName);
        return builder.build();
    }
}
