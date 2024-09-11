//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stream;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.processor.sink.Coordinator;
import io.deephaven.processor.sink.Proxy;
import io.deephaven.processor.sink.Proxy.StreamingTarget;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

public class Example {

    public interface MyStuff {
        void foo(int x);

        void bar(String x);

        void advanceAll();
    }

    @Immutable
    @SimpleStyle
    public static abstract class Combo {

        @Parameter
        public abstract Table table();

        @Parameter
        public abstract Coordinator coordinator();

        @Parameter
        public abstract MyStuff i();
    }

    public static Combo combo() {
        final StreamingTarget<MyStuff> target = Proxy.streamingTarget(MyStuff.class);
        final Chunks chunks = new Chunks(target.types());
        final MyStuff bound = target.bind(chunks);
        final TableDefinition tableDefinition = TableDefinition.of(
                ColumnDefinition.of("Col1", target.types().get(0)),
                ColumnDefinition.of("Col2", target.types().get(1)));
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(tableDefinition, chunks,
                ExecutionContext.getContext().getUpdateGraph(), "test");
        final Table blink = adapter.table();
        return ImmutableCombo.of(blink.tail(100), chunks, bound);
    }
}
