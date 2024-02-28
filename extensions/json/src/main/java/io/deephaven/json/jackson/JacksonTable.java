/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.json.ArrayOptions;
import io.deephaven.json.JsonStreamPublisher;
import io.deephaven.json.JsonStreamPublisherOptions;
import io.deephaven.json.JsonTableOptions;
import io.deephaven.json.Source;
import io.deephaven.json.ValueOptions;
import io.deephaven.json.jackson.NavContext.JsonProcess;
import io.deephaven.json.jackson.PathToSingleValue.Results;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class JacksonTable {

    public static Table execute(JsonTableOptions options) {
        return execute(options, JacksonConfiguration.defaultFactory());
    }

    public static Table execute(JsonTableOptions options, JsonFactory factory) {
        final ThreadFactory threadFactory = Executors.defaultThreadFactory();
        final int numThreads = Math.min(options.maxThreads(), options.sources().size());
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads, threadFactory);
        try {
            return execute(options, factory, executor, numThreads > 1);
        } finally {
            // ensure no new tasks can be added, but does not cancel existing submissions
            executor.shutdown();
        }
    }

    private static Table execute(JsonTableOptions options, JsonFactory factory, Executor executor, boolean concurrent) {
        final JacksonStreamPublisher publisher = publisher(JsonStreamPublisherOptions.builder()
                .options(options.options())
                .multiValueSupport(options.multiValueSupport())
                .chunkSize(options.chunkSize())
                .build());
        final TableDefinition tableDefinition = publisher.tableDefinition(options.namingFunction());
        // noinspection resource
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(tableDefinition, publisher,
                options.updateSourceRegistrar(), options.name(), options.extraAttributes(), true);
        if (concurrent) {
            for (final Source source : options.sources()) {
                publisher.execute(executor, List.of(source), factory);
            }
        } else {
            publisher.execute(executor, options.sources(), factory);
        }
        return adapter.table();
    }

    public static JacksonStreamPublisher publisher(JsonStreamPublisherOptions options) {
        // todo: we should probably do this for kafka as well
        final Results results = PathToSingleValue.of(options.options());
        final ValueOptions singleValue = results.options();
        final ValueOptions element;
        final boolean isArray;
        if (singleValue instanceof ArrayOptions) {
            isArray = true;
            element = ((ArrayOptions) singleValue).element();
        } else {
            isArray = false;
            element = singleValue;
        }
        return new JacksonStreamPublisher(results.path(), isArray, element, options.chunkSize(),
                options.multiValueSupport());
    }

    public static final class JacksonStreamPublisher implements JsonStreamPublisher {
        private final List<String> path;
        private final boolean pathIsToArray;
        private final int chunkSize;
        private final boolean multiValue;
        private final Mixin<?> elementMixin;
        private final List<Type<?>> chunkTypes;
        private StreamConsumer consumer;
        private volatile boolean shutdown;

        public JacksonStreamPublisher(
                List<String> path,
                boolean pathIsToArray,
                ValueOptions elementOptions,
                int chunkSize,
                boolean multiValue) {
            this.path = List.copyOf(path);
            this.pathIsToArray = pathIsToArray;
            this.chunkSize = chunkSize;
            this.multiValue = multiValue;
            // note: this is the motivating reason to remove factory from mixin
            this.elementMixin = Objects.requireNonNull(Mixin.of(elementOptions, null));
            this.chunkTypes = elementMixin.outputTypes().collect(Collectors.toList());
        }

        public TableDefinition tableDefinition(Function<List<String>, String> namingFunction) {
            return TableDefinition.from(elementMixin.names(namingFunction), chunkTypes);
        }

        @Override
        public void register(@NotNull StreamConsumer consumer) {
            this.consumer = Objects.requireNonNull(consumer);
        }

        @Override
        public void flush() {
            // unlike async streaming cases, the runners here are going as fast as possible, and hand off their buffers
            // asap.
        }

        @Override
        public void shutdown() {
            this.shutdown = true;
        }

        @Override
        public Runnable runnable(List<Source> sources) {
            return runnable(sources, JacksonConfiguration.defaultFactory());
        }

        public Runnable runnable(List<Source> sources, JsonFactory factory) {
            if (consumer == null) {
                throw new IllegalStateException("Must register a consumer first");
            }
            return new Runner(factory, sources);
        }

        public void execute(Executor executor, List<Source> sources, JsonFactory factory) {
            executor.execute(runnable(sources, factory));
        }

        private class Runner implements Runnable, JsonProcess {
            private final JsonFactory factory;
            private final List<Source> sources;
            private final WritableChunk[] outArray;
            private List<WritableChunk<?>> out;
            private ValueProcessor elementProcessor;
            private int count;

            public Runner(JsonFactory factory, List<Source> sources) {
                if (sources.isEmpty()) {
                    throw new IllegalArgumentException("sources must be non-empty");
                }
                this.factory = Objects.requireNonNull(factory);
                this.sources = List.copyOf(sources);
                this.outArray = new WritableChunk[chunkTypes.size()];
                out = newProcessorChunks(chunkSize, chunkTypes);
                count = 0;
                elementProcessor = elementMixin.processor("todo", out);
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
                for (final Source source : sources) {
                    try (final JsonParser parser = JacksonSource.of(factory, source)) {
                        parser.nextToken();
                        runImpl(parser);
                    }
                }
                if (count != 0) {
                    flushImpl();
                }
            }

            private void runImpl(JsonParser parser) throws IOException {
                do {
                    NavContext.processObjectField(parser, path, this);
                } while (multiValue && !parser.hasToken(null));
                // todo: throw exception if not multi value and not null?
            }

            @Override
            public void process(JsonParser parser) throws IOException {
                if (pathIsToArray) {
                    processArrayOfElements(parser);
                } else {
                    processElement(parser);
                }
            }

            private void processArrayOfElements(JsonParser parser) throws IOException {
                if (!parser.hasToken(JsonToken.START_ARRAY)) {
                    throw new IllegalStateException();
                }
                parser.nextToken();
                while (!parser.hasToken(JsonToken.END_ARRAY)) {
                    processElement(parser);
                }
                parser.nextToken();
            }

            private void processElement(JsonParser parser) throws IOException {
                elementProcessor.processCurrentValue(parser);
                parser.nextToken();
                if (shutdown) {
                    throw new IOException("shutdown");
                }
                if (++count == chunkSize) {
                    flushImpl();
                }
            }

            private void flushImpl() {
                // noinspection unchecked
                consumer.accept(out.toArray(outArray));
                out = newProcessorChunks(chunkSize, chunkTypes);
                count = 0;
                elementProcessor = elementMixin.processor("todo", out);
            }
        }
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
