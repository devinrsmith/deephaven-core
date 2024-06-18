//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.processor;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

@Immutable
@BuildableStyle
public abstract class TableStreamPublisherOptions {
    public abstract Table table();

    /**
     * The input column name. When unset, the first column from {@link #table()} will be used.
     */
    public abstract Optional<String> columnName();

    /**
     * The named object processor provider. Must be capable of handling the input column type.
     */
    public abstract NamedObjectProcessor.Provider processor();

    /**
     * The chunk size used to iterate through {@link #table()}. By default, is
     * {@value ArrayBackedColumnSource#BLOCK_SIZE}.
     */
    @Default
    public int chunkSize() {
        return ArrayBackedColumnSource.BLOCK_SIZE;
    }

    public final Table execute() {



        return null;
    }

    private <T> Table what(ColumnSource<T> columnSource) {
        final NamedObjectProcessor<? super T> processor = processor().named(columnSource.type());
        final QueryTable newTable = null;
        final StreamToBlinkTableAdapter adapter =
                new StreamToBlinkTableAdapter(null, null, table().getUpdateGraph(), "test");
        final TableProcessorStreamPublisher<T> streamPublisher = new TableProcessorStreamPublisher<>("todo", table(),
                newTable, columnSource, processor.processor(), chunkSize());


        return null;

        /*
         * final JsonStreamPublisher publisher = options.execute(); final TableDefinition tableDefinition =
         * publisher.tableDefinition(JsonTableOptions::toColumnName); final StreamToBlinkTableAdapter adapter = new
         * StreamToBlinkTableAdapter(tableDefinition, publisher, parent.getUpdateGraph(), name, Map.of(), true);
         */
    }
}
