//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.TableDefinition;
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
        return new JacksonStreamPublisher(results.path(), results.options(), options.chunkSize(),
                options.multiValueSupport(), factory);
    }

    private final List<Path> path;
    private final int chunkSize;
    private final boolean multiValue;
    private final JsonFactory factory;
    private final Mixin<?> mixin;
    private final List<Type<?>> chunkTypes;
    private StreamConsumer consumer;
    private volatile boolean shutdown;

    private JacksonStreamPublisher(
            List<Path> path,
            ValueOptions options,
            int chunkSize,
            boolean multiValue,
            JsonFactory factory) {
        this.path = List.copyOf(path);
        this.chunkSize = chunkSize;
        this.multiValue = multiValue;
        this.factory = Objects.requireNonNull(factory);
        this.mixin = Objects.requireNonNull(Mixin.of(options, factory));
        this.chunkTypes = types();
    }

    public TableDefinition tableDefinition(Function<List<String>, String> namingFunction) {
        return TableDefinition.from(mixin.names(namingFunction), chunkTypes);
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
        executor.execute(runner(sources));
    }

    private List<Type<?>> types() {
        if (mixin instanceof ArrayMixin) {
            return ((ArrayMixin) mixin).elementOutputTypes().collect(Collectors.toList());
        }
        if (mixin instanceof ObjectKvMixin) {
            return ((ObjectKvMixin) mixin).keyValueOutputTypes().collect(Collectors.toList());
        }
        return mixin.outputTypes().collect(Collectors.toList());
    }

    private Runnable runner(Queue<Source> sources) {
        if (mixin instanceof ArrayMixin) {
            return new ArrayRunner(sources);
        }
        if (mixin instanceof ObjectKvMixin) {
            return new KvRunner(sources);
        }
        return new NormalRunner(sources);
    }

    private abstract class StatefulRunner implements Runnable, JsonProcess {
        private final Queue<Source> sources;
        @SuppressWarnings("rawtypes")
        private final WritableChunk[] outArray;
        private List<WritableChunk<?>> out;
        private int count;

        public StatefulRunner(Queue<Source> sources) {
            this.sources = Objects.requireNonNull(sources);
            this.outArray = new WritableChunk[chunkTypes.size()];
            out = newProcessorChunks(chunkSize, chunkTypes);
            count = 0;
            prepare(out);
        }

        public abstract void prepare(List<WritableChunk<?>> out);

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

        void incAndMaybeFlush() {
            if (shutdown) {
                throw new RuntimeException("shutdown");
            }
            if (++count == chunkSize) {
                // todo: this may flush intra-source
                flushImpl();
            }
        }

        private void flushImpl() {
            // noinspection unchecked
            consumer.accept(out.toArray(outArray));
            out = newProcessorChunks(chunkSize, chunkTypes);
            count = 0;
            prepare(out);
        }
    }

    private class NormalRunner extends StatefulRunner {
        private ValueProcessor processor;

        public NormalRunner(Queue<Source> sources) {
            super(sources);
        }

        @Override
        public void prepare(List<WritableChunk<?>> out) {
            processor = mixin.processor(NormalRunner.class.getName(), out);
        }

        @Override
        public void process(JsonParser parser) throws IOException {
            if (parser.hasToken(null)) {
                processor.processMissing(parser);
            } else {
                processor.processCurrentValue(parser);
            }
            incAndMaybeFlush();
            parser.nextToken();
        }
    }

    private class ArrayRunner extends StatefulRunner {
        private ValueProcessor processor;

        public ArrayRunner(Queue<Source> sources) {
            super(sources);
        }

        @Override
        public void prepare(List<WritableChunk<?>> out) {
            processor = ((ArrayMixin) mixin).elementAsValueProcessor(out);
        }

        @Override
        public void process(JsonParser parser) throws IOException {
            ValueProcessorArrayImpl.processArray2(processor, parser, this::incAndMaybeFlush);
            parser.nextToken();
        }
    }

    private class KvRunner extends StatefulRunner {
        private ValueProcessor keyProcessor;
        private ValueProcessor valueProcessor;

        public KvRunner(Queue<Source> sources) {
            super(sources);
        }

        @Override
        public void prepare(List<WritableChunk<?>> out) {
            keyProcessor = ((ObjectKvMixin) mixin).keyAsValueProcessor(out);
            valueProcessor = ((ObjectKvMixin) mixin).valueAsValueProcessor(out);
        }

        @Override
        public void process(JsonParser parser) throws IOException {
            ValueProcessorKvImpl.processKeyValues2(keyProcessor, valueProcessor, parser, this::incAndMaybeFlush);
            parser.nextToken();
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
