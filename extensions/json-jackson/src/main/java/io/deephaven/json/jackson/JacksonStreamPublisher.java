/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.json.ArrayOptions;
import io.deephaven.json.JsonStreamPublisher;
import io.deephaven.json.JsonStreamPublisherOptions;
import io.deephaven.json.Source;
import io.deephaven.json.ValueOptions;
import io.deephaven.json.jackson.NavContext.JsonProcess;
import io.deephaven.json.jackson.PathToSingleValue.Path;
import io.deephaven.json.jackson.PathToSingleValue.Results;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class JacksonStreamPublisher implements JsonStreamPublisher {
    public static JacksonStreamPublisher of(JsonStreamPublisherOptions options, JsonFactory factory) {
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
                options.multiValueSupport(), factory);
    }

    private final List<Path> path;
    private final boolean pathIsToArray;
    private final int chunkSize;
    private final boolean multiValue;
    private final JsonFactory factory;
    private final Mixin<?> elementMixin;
    private final List<Type<?>> chunkTypes;
    private StreamConsumer consumer;
    private volatile boolean shutdown;

    private JacksonStreamPublisher(
            List<Path> path,
            boolean pathIsToArray,
            ValueOptions elementOptions,
            int chunkSize,
            boolean multiValue,
            JsonFactory factory) {
        this.path = List.copyOf(path);
        this.pathIsToArray = pathIsToArray;
        this.chunkSize = chunkSize;
        this.multiValue = multiValue;
        this.factory = Objects.requireNonNull(factory);
        this.elementMixin = Objects.requireNonNull(Mixin.of(elementOptions, factory));
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
    public void execute(Executor executor, Queue<Source> sources) {
        executor.execute(new StatefulRunner(sources));
    }

    private class StatefulRunner implements Runnable, JsonProcess {
        private final Queue<Source> sources;
        @SuppressWarnings("rawtypes")
        private final WritableChunk[] outArray;
        private List<WritableChunk<?>> out;
        private ValueProcessor elementProcessor;
        private int count;

        public StatefulRunner(Queue<Source> sources) {
            this.sources = Objects.requireNonNull(sources);
            this.outArray = new WritableChunk[chunkTypes.size()];
            out = newProcessorChunks(chunkSize, chunkTypes);
            count = 0;
            elementProcessor = elementMixin.processor(StatefulRunner.class.getName(), out);
        }

        @Override
        public void run() {
            try {
                runImpl();
            } catch (Throwable t) {
                consumer.acceptFailure(t);
            }
        }

        private void runImpl() throws IOException {
            Source source;
            while ((source = sources.poll()) != null) {
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
                NavContext.processPath(parser, path, this);
            } while (multiValue && !parser.hasToken(null));
            // todo: throw exception if not multi value and not null?
        }

        @Override
        public void process(JsonParser parser) throws IOException {
            if (pathIsToArray) {
                processArrayOfElements(parser);
            } else if (parser.hasToken(null)) {
                processMissingElement(parser);
            } else {
                processElement(parser);
            }
            parser.nextToken();
        }

        private void processArrayOfElements(JsonParser parser) throws IOException {
            if (!parser.hasToken(JsonToken.START_ARRAY)) {
                throw new IllegalStateException();
            }
            parser.nextToken();
            while (!parser.hasToken(JsonToken.END_ARRAY)) {
                processElement(parser);
                parser.nextToken();
            }
        }

        private void processElement(JsonParser parser) throws IOException {
            elementProcessor.processCurrentValue(parser);
            incAndMaybeFlush();
        }

        private void processMissingElement(JsonParser parser) throws IOException {
            elementProcessor.processMissing(parser);
            incAndMaybeFlush();
        }

        private void incAndMaybeFlush() throws IOException {
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

    private static List<WritableChunk<?>> newProcessorChunks(int chunkSize, List<Type<?>> types) {
        return types
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(chunkType -> chunkType.makeWritableChunk(chunkSize))
                .peek(wc -> wc.setSize(0))
                .collect(Collectors.toList());
    }
}
