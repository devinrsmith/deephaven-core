//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stream;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.processor.sink.Coordinator;
import io.deephaven.processor.sink.Proxy;
import io.deephaven.processor.sink.Proxy.StreamingTarget;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.InstantAppender;
import io.deephaven.processor.sink.appender.IntAppender;
import io.deephaven.processor.sink.appender.LongAppender;
import io.deephaven.processor.sink.appender.ObjectAppender;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class Example {

    public interface MyStuff {
        void foo(int x);

        void bar(String x);

        void baz(long millis);

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

    public static Combo viaProxy() {
        final StreamingTarget<MyStuff> target = Proxy.streamingTarget(MyStuff.class);
        final SingleBlinkCoordinator coord = new SingleBlinkCoordinator(target.types());
        final MyStuff bound = target.bind(coord);
        // todo: this is a manual definition hack for now, would not be needed w/ further integration work
        final TableDefinition tableDefinition = TableDefinition.of(
                ColumnDefinition.of("Col1", target.types().get(0)),
                ColumnDefinition.of("Col2", target.types().get(1)),
                ColumnDefinition.of("Col3", target.types().get(2)));
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(tableDefinition, coord,
                ExecutionContext.getContext().getUpdateGraph(), "test");
        final Table blink = adapter.table();
        return ImmutableCombo.of(blink, coord, bound);
    }

    public static Combo viaManual() {
        final SingleBlinkCoordinator coord =
                new SingleBlinkCoordinator(List.of(Type.intType(), Type.stringType(), Type.instantType()));
        final TableDefinition tableDefinition = TableDefinition.of(
                ColumnDefinition.ofInt("Foo"),
                ColumnDefinition.ofString("Bar"),
                ColumnDefinition.ofTime("Baz"));
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(tableDefinition, coord,
                ExecutionContext.getContext().getUpdateGraph(), "test");
        final Table blink = adapter.table();
        return ImmutableCombo.of(blink, coord, new MyStuffManual(coord));
    }

    static class MyStuffManual implements MyStuff {
        private final Stream stream;
        private final IntAppender foo;
        private final ObjectAppender<String> bar;
        private final LongAppender bazMillis;

        public MyStuffManual(Stream stream) {
            this.stream = Objects.requireNonNull(stream);
            this.foo = IntAppender.get(stream.appenders().get(0));
            this.bar = ObjectAppender.get(stream.appenders().get(1), Type.stringType());
            this.bazMillis = InstantAppender.get(stream.appenders().get(2)).asLongEpochAppender(TimeUnit.MILLISECONDS);
        }

        @Override
        public void foo(int x) {
            this.foo.set(x);
        }

        @Override
        public void bar(String x) {
            this.bar.set(x);
        }

        @Override
        public void baz(long millis) {
            this.bazMillis.set(millis);
        }

        @Override
        public void advanceAll() {
            stream.advanceAll();
        }
    }

    static class MyStuffChunkBased implements MyStuff {

        private WritableIntChunk<?> foo;
        private WritableObjectChunk<String, ?> bar;
        private WritableLongChunk<?> bazMillis;

        @Override
        public void foo(int x) {

        }

        @Override
        public void bar(String x) {

        }

        @Override
        public void baz(long millis) {

        }

        @Override
        public void advanceAll() {

        }
    }
}
