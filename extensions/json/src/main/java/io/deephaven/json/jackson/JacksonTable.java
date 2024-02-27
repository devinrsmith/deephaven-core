/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.json.ArrayOptions;
import io.deephaven.json.IntOptions;
import io.deephaven.json.JsonTableOptions;
import io.deephaven.json.LongOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.Source;
import io.deephaven.json.StringOptions;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static io.deephaven.json.jackson.Helpers.assertCurrentToken;
import static io.deephaven.json.jackson.Helpers.assertNextToken;
import static io.deephaven.json.jackson.Mixin.TO_COLUMN_NAME;

public final class JacksonTable {

    public static Table execute(String source) {
        return execute(JsonTableOptions.builder()
                .options(ObjectOptions.builder()
                        .putFields("name", StringOptions.strict())
                        .putFields("age", IntOptions.strict())
                        .build())
                .source(Path.of(source))
                .multiValueSupport(true)
                .build());
    }

    // https://raw.githubusercontent.com/dariusk/corpora/master/data/colors/dulux.json
    public static Table dulux() {
        return execute(JsonTableOptions.builder()
                .options(ArrayOptions.strict(ObjectOptions.builder()
                        .putFields("name", StringOptions.strict())
                        .putFields("code", StringOptions.strict())
                        .putFields("lrv", StringOptions.strict())
                        .putFields("id", LongOptions.lenient())
                        .putFields("r", IntOptions.lenient())
                        .putFields("g", IntOptions.lenient())
                        .putFields("b", IntOptions.lenient())
                        .build()))
                .source(Path.of("/tmp/dulux.json"))
                .build());
    }

    public static Table execute(JsonTableOptions options) {
        return execute(JacksonConfiguration.defaultFactory(), options);
    }

    public static Table execute(JsonFactory factory, JsonTableOptions options) {
        // todo: what about a version that blocks until complete?
        final Mixin mixin = Mixin.of(options.options(), factory);
        final Mixin element = element(mixin);
        final JsonStreamPublisher publisher = new JsonStreamPublisher(mixin, options.chunkSize(), options.multiValueSupport());
        final TableDefinition td = TableDefinition.from(element.names(TO_COLUMN_NAME), element.outputTypes().collect(Collectors.toList()));
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(td, publisher, ExecutionContext.getContext().getUpdateGraph(), "todo");
        publisher.submit(Executors.newSingleThreadExecutor(), factory, options.source());
        return adapter.table();
    }

    private static Mixin element(Mixin mixin) {
        return mixin instanceof ArrayMixin ? ((ArrayMixin) mixin).element() : mixin;
    }

    static class JsonStreamPublisher implements StreamPublisher {
        private final Mixin mixin;
        private final int chunkSize;
        private final boolean multiValue;
        private StreamConsumer consumer;
        private volatile boolean shutdown;

        public JsonStreamPublisher(Mixin mixin, int chunkSize, boolean multiValue) {
            this.mixin = Objects.requireNonNull(mixin);
            this.chunkSize = chunkSize;
            this.multiValue = multiValue;
        }

        @Override
        public void register(@NotNull StreamConsumer consumer) {
            this.consumer = Objects.requireNonNull(consumer);
        }

        @Override
        public void flush() {

        }

        @Override
        public void shutdown() {
            this.shutdown = true;
        }

        public void submit(Executor e, JsonFactory factory, Source source) {
            e.execute(runner(factory, source));
        }

        private Runnable runner(JsonFactory factory, Source source) {
            if (multiValue) {
                return new MultiValueRunner(factory, source);
            }
            if (mixin instanceof ArrayMixin) {
                return new ArrayRunner(factory, source);
            }
            throw new RuntimeException("todo");
        }

        private class MultiValueRunner extends Runner {
            public MultiValueRunner(JsonFactory factory, Source source) {
                super(factory, source);
            }

            @Override
            void parseImpl(JsonParser parser) throws IOException {
                // first token depends on mixin impl
                loop(parser);
                // always expect null token at end
                assertCurrentToken(parser, null);
            }
        }

        private class ArrayRunner extends Runner {
            public ArrayRunner(JsonFactory factory, Source source) {
                super(factory, source);
            }

            @Override
            void parseImpl(JsonParser parser) throws IOException {
                assertNextToken(parser, JsonToken.START_ARRAY);
                loop(parser);
                assertCurrentToken(parser, JsonToken.END_ARRAY);
                // todo: what happens if the user wants to keep processing delimited arrays?
            }
        }

        private abstract class Runner implements Runnable {

            private final JsonFactory factory;
            private final Source source;

            public Runner(JsonFactory factory, Source source) {
                this.factory = Objects.requireNonNull(factory);
                this.source = Objects.requireNonNull(source);
            }

            @Override
            public void run() {
                try {
                    runImpl();
                } catch (Throwable t) {
                    consumer.acceptFailure(t);
                }
                // todo: notify consumer on shutdown?
            }

            private void runImpl() throws IOException {
                try (final JsonParser parser = JacksonSource.of(factory, source)) {
                    parseImpl(parser);
                }
            }

            abstract void parseImpl(JsonParser parser) throws IOException;
        }

        private void loop(JsonParser parser) throws IOException {
            final Mixin element = element(mixin);
            final List<Type<?>> outputTypes = element.outputTypes().collect(Collectors.toList());
            //noinspection rawtypes
            final WritableChunk[] outArrays = new WritableChunk[outputTypes.size()];
            while (!shutdown) {
                // TODO: need richer interface w/ unknown size to begin with
                // todo: blink table vs static
                final List<WritableChunk<?>> out = newProcessorChunks(chunkSize, outputTypes);
                final ValueProcessor processor = element.processor("todo", out);
                int i;
                for (i = 0; i < chunkSize && isValue(parser.nextToken()); ++i) {
                    processor.processCurrentValue(parser);
                }
                if (i == 0) {
                    return;
                }
                //noinspection unchecked
                consumer.accept(out.toArray(outArrays));
                if (i < chunkSize) {
                    return;
                }
            }
        }
    }

    private static boolean isValue(JsonToken token) {
        if (token == null) {
            return false;
        }
        switch (token) {
            case START_OBJECT:
            case START_ARRAY:
            case VALUE_STRING:
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
            case VALUE_TRUE:
            case VALUE_FALSE:
            case VALUE_NULL:
                return true;
        }
        return false;
    }

    private static List<WritableChunk<?>> newProcessorChunks(int chunkSize, List<Type<?>> types) {
        return types
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(chunkType -> chunkType.makeWritableChunk(chunkSize))
                .peek(wc -> wc.setSize(0))
                .collect(Collectors.toList());
    }
}
