//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stream;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.BaseTable.ListenerImpl;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.processor.NamedObjectProcessor;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@BuildableStyle
@Immutable
public abstract class ObjectProcessorBlinkTableOptions<T> {

    public static <T> Builder<T> builder() {
        return ImmutableObjectProcessorBlinkTableOptions.builder();
    }

    public abstract Table table();

    public abstract String columnName();

    public abstract Class<T> columnType();

    public abstract NamedObjectProcessor<T> processor();

    @Default
    public boolean publishInitial() {
        return true;
    }

    @Default
    public String name() {
        return UUID.randomUUID().toString();
    }

    @Default
    public int chunkSize() {
        return ArrayBackedColumnSource.BLOCK_SIZE;
    }

    @Default
    public UpdateSourceRegistrar updateSourceRegistrar() {
        // todo: we _can_ pass to another update graph for result, right?
        // would we need to use something besides ListenerImpl?
        return table().getUpdateGraph();
    }

    public abstract Map<String, Object> extraAttributes();

    public final Table execute() {
        final ObjectProcessorStreamPublisher<T> publisher =
                new ObjectProcessorStreamPublisher<>(processor().processor());
        final TableDefinition tableDefinition =
                TableDefinition.from(processor().columnNames(), processor().processor().outputTypes());
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(tableDefinition, publisher,
                updateSourceRegistrar(), name(), extraAttributes(), true);
        final Table blinkTable = adapter.table();
        final Listener listener = new Listener((BaseTable<?>) blinkTable, publisher);
        table().addUpdateListener(listener);
        if (publishInitial()) {
            // we can probably do better than this with ConstructSnapshot?
            listener.publishInitial();
        }
        return blinkTable;
    }

    public interface Builder<T> {
        Builder<T> table(Table table);

        Builder<T> columnName(String columnName);

        Builder<T> columnType(Class<T> columnType);

        Builder<T> processor(NamedObjectProcessor<T> namedObjectProcessor);

        Builder<T> publishInitial(boolean publishInitial);

        Builder<T> name(String name);

        Builder<T> chunkSize(int chunkSize);

        Builder<T> updateSourceRegistrar(UpdateSourceRegistrar updateSourceRegistrar);

        Builder<T> putExtraAttributes(String key, Object value);

        Builder<T> putExtraAttributes(Map.Entry<String, ? extends Object> entry);

        Builder<T> putAllExtraAttributes(Map<String, ? extends Object> entries);

        ObjectProcessorBlinkTableOptions<T> build();
    }

    private class Listener extends ListenerImpl {

        private final ObjectProcessorStreamPublisher<T> publisher;
        private final ColumnSource<T> columnSource;

        public Listener(BaseTable<?> dependent, ObjectProcessorStreamPublisher<T> publisher) {
            super(name() + "-Listener", table(), dependent);
            this.publisher = Objects.requireNonNull(publisher);
            this.columnSource = table().getColumnSource(columnName(), columnType());
        }

        public void publishInitial() {
            if (table().isRefreshing()) {
                table().getUpdateGraph().checkInitiateSerialTableOperation();
            }
            publisher.execute(columnSource, table().getRowSet(), chunkSize(), false);
        }

        @Override
        public void onUpdate(TableUpdate upstream) {
            // todo: config modified / removed
            // todo: shutdown?
            // table().removeUpdateListener(this);
            publisher.execute(columnSource, upstream.added(), chunkSize(), false);
            // todo: no need to pass rowsets, right?
            // should this be ListenerImpl?
        }
    }

    @Check
    final void checkInitial() {
        if (!publishInitial() && !table().isRefreshing()) {
            throw new IllegalArgumentException();
        }
    }
}
