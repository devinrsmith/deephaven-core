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
import io.deephaven.json.InstantNumberOptions;
import io.deephaven.json.InstantOptions;
import io.deephaven.json.IntOptions;
import io.deephaven.json.JsonTableOptions;
import io.deephaven.json.LongOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.Source;
import io.deephaven.json.StringOptions;
import io.deephaven.json.ValueOptions;
import io.deephaven.json.jackson.NavContext.JsonProcess;
import io.deephaven.json.jackson.PathToSingleValue.Results;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

import static io.deephaven.json.jackson.Mixin.TO_COLUMN_NAME;

public final class JacksonTable {

    public static Table largeFile(String source) {
        return execute(JsonTableOptions.builder()
                .options(ArrayOptions.strict(ObjectOptions.builder()
                        .putFields("id", LongOptions.lenient())
                        .putFields("type", StringOptions.strict())
                        .putFields("created_at", InstantOptions.strict())
                        .build()))
                .addSources(Path.of(source))
                .build());
    }

    public static Table execute(String source) {
        return execute(JsonTableOptions.builder()
                .options(ObjectOptions.builder()
                        .putFields("id", StringOptions.strict())
                        .putFields("age", IntOptions.strict())
                        .build())
                .addSources(Path.of(source))
                .multiValueSupport(true)
                .build());
    }

    public static Table unconfirmedTransactions() throws MalformedURLException {
        return execute(JsonTableOptions.builder()
                .addSources(new URL("https://blockchain.info/unconfirmed-transactions?format=json"))
                .options(ObjectOptions.builder()
                        .putFields("txs", ArrayOptions.builder()
                                .element(ObjectOptions.builder()
                                        .putFields("hash", StringOptions.strict())
                                        .putFields("ver", IntOptions.strict())
                                        .putFields("vin_sz", IntOptions.strict())
                                        .putFields("vout_sz", IntOptions.strict())
                                        .putFields("size", IntOptions.strict())
                                        .putFields("weight", IntOptions.strict())
                                        .putFields("fee", LongOptions.strict())
                                        .putFields("relayed_by", StringOptions.strict())
                                        .putFields("lock_time", LongOptions.strict())
                                        .putFields("tx_index", LongOptions.strict())
                                        // todo double_spend bool
                                        .putFields("time", InstantNumberOptions.Format.EPOCH_SECONDS.strict())
                                        // todo: inputs
                                        // todo: outputs
                                        .build())
                                .build())
                        .build())
                .multiValueSupport(false)
                .build());
    }

    public static Table latestBlock() throws MalformedURLException {
        return execute(JsonTableOptions.builder()
                .addSources(new URL("https://blockchain.info/latestblock"))
                .options(ObjectOptions.builder()
                        .putFields("txIndexes", ArrayOptions.strict(LongOptions.strict()))
                        .build())
                .build());
    }

    // https://raw.githubusercontent.com/dariusk/corpora/master/data/colors/dulux.json
    public static Table dulux() throws MalformedURLException {
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
                .addSources(new URL("https://raw.githubusercontent.com/dariusk/corpora/master/data/colors/dulux.json"))
                .build());
    }

    public static Table cloudTrails() {
        return execute(JsonTableOptions.builder()
                .options(ObjectOptions.builder()
                        .putFields("Records", ArrayOptions.strict(ObjectOptions.builder()
                                .putFields("userAgent", StringOptions.standard())
                                .putFields("eventID", StringOptions.strict())
                                .putFields("userIdentity", ObjectOptions.builder()
                                        .putFields("type", StringOptions.standard())
                                        .putFields("principalId", StringOptions.standard())
                                        .putFields("arn", StringOptions.standard())
                                        .putFields("accountId", StringOptions.standard())
                                        .putFields("accessKeyId", StringOptions.standard())
                                        .putFields("sessionContext", ObjectOptions.builder()
                                                .putFields("attributes", ObjectOptions.builder()
                                                        // todo bool lenient
                                                        .putFields("mfaAuthenticated", StringOptions.standard())
                                                        .putFields("creationDate", InstantOptions.standard())
                                                        .build())
                                                .build())
                                        .build())
                                .putFields("errorMessage", StringOptions.standard())
                                .putFields("eventType", StringOptions.strict())
                                .putFields("sourceIPAddress", StringOptions.strict())
                                .putFields("eventName", StringOptions.strict())
                                .putFields("eventSource", StringOptions.strict())
                                .putFields("recipientAccountId", LongOptions.lenient())
                                .putFields("awsRegion", StringOptions.strict())
                                .putFields("requestID", StringOptions.standard())
                                .putFields("eventVersion", StringOptions.strict())
                                .putFields("eventTime", InstantOptions.strict())
                                .build()))
                        .build())
                // .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail.json.nd"))
                // .multiValueSupport(true)
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail00.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail01.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail02.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail03.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail04.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail05.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail06.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail07.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail08.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail09.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail10.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail11.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail12.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail13.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail14.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail15.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail16.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail17.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail18.json"))
                .addSources(Path.of("/home/devin/Downloads/flaws_cloudtrail_logs/flaws_cloudtrail19.json"))
                .build());
    }

    public static Table execute(JsonTableOptions options) {
        return execute(options, JacksonConfiguration.defaultFactory());
    }

    public static Table execute(JsonTableOptions options, JsonFactory factory) {
        final ThreadFactory threadFactory = Executors.defaultThreadFactory();
        final int numThreads = Math.min(options.maxThreads(), options.sources().size());
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads, threadFactory);
        try {
            return execute(options, factory, executor);
        } finally {
            // ensure no new tasks can be added, but does not cancel existing submission
            executor.shutdown();
        }
    }

    private static Table execute(JsonTableOptions options, JsonFactory factory, ExecutorService executorService) {
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
        final JsonStreamPublisher publisher = new JsonStreamPublisher(results.path(), isArray, element,
                options.chunkSize(), options.multiValueSupport());
        // noinspection resource
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(publisher.tableDefinition(), publisher,
                options.updateSourceRegistrar(), options.name());
        for (Source source : options.sources()) {
            // todo: let caller know when done
            publisher.submit(executorService, factory, source);
        }
        return adapter.table();
    }

    static class JsonStreamPublisher implements StreamPublisher {

        private final List<String> path;
        private final boolean pathIsToArray;
        private final Mixin<?> elementMixin;
        private final List<Type<?>> chunkTypes;
        private final int chunkSize;
        private final boolean multiValue;
        private StreamConsumer consumer;

        private volatile boolean shutdown;

        public JsonStreamPublisher(List<String> path, boolean pathIsToArray, ValueOptions elementOptions, int chunkSize,
                boolean multiValue) {
            this.path = List.copyOf(path);
            this.pathIsToArray = pathIsToArray;
            this.chunkSize = chunkSize;
            this.multiValue = multiValue;
            // note: this is the motivating reason to remove factory from mixin
            this.elementMixin = Objects.requireNonNull(Mixin.of(elementOptions, null));
            this.chunkTypes = elementMixin.outputTypes().collect(Collectors.toList());
        }

        public TableDefinition tableDefinition() {
            return TableDefinition.from(elementMixin.names(TO_COLUMN_NAME), chunkTypes);
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
            // todo cancel future
        }

        public Future<?> submit(ExecutorService e, JsonFactory factory, Source source) {
            return e.submit(new Runner(factory, source));
        }

        // private Runnable runner(JsonFactory factory, Source source) {
        //
        // final Results results = SinglePathToValue.of(elementMixin.options());
        // final ValueOptions singleValue = results.options();
        // final ValueOptions element;
        // final boolean isArray;
        // if (singleValue instanceof ArrayOptions) {
        // isArray = true;
        // element = ((ArrayOptions)singleValue).element();
        // } else {
        // isArray = false;
        // element = singleValue;
        // }
        ////
        //// final JsonProcess delegate = NavContext.singleFieldProcess(results.path(), new JsonProcess() {
        //// @Override
        //// public void process(JsonParser parser) throws IOException {
        ////
        //// }
        //// });
        ////
        //// return new Runner2(factory, source, multiValue(delegate));
        // }


        // private void runImpl(JsonParser parser) throws IOException {
        // // value based
        // // <delegate>
        //
        // // array based
        // // [ <delegate> ]
        //
        // // delimiter value based
        // // <delegate>
        // // <delegate>
        //
        // // ref to value
        // // { "ref": <delegate> }
        //
        // // delimiter and ref
        // // { "ref": <delegate> }
        // // { "ref": <delegate> }
        //
        // // todo: any of the above (except delimiter) nested as first element of array
        // // [ <above> ]
        //
        // NavContext.processObjectField(parser, List.of(), delegate);
        // }

        private class Runner implements Runnable, JsonProcess {

            private final JsonFactory factory;
            private final Source source;
            private final WritableChunk[] outArray;
            private List<WritableChunk<?>> out;
            private ValueProcessor elementProcessor;
            private int count;

            public Runner(JsonFactory factory, Source source) {
                this.factory = Objects.requireNonNull(factory);
                this.source = Objects.requireNonNull(source);
                this.outArray = new WritableChunk[chunkTypes.size()];
                out = newProcessorChunks(chunkSize, chunkTypes);
                count = 0;
                elementProcessor = elementMixin.processor("todo", out);
            }

            @Override
            public void run() {
                try (final JsonParser parser = JacksonSource.of(factory, source)) {
                    parser.nextToken();
                    runImpl(parser);
                } catch (Throwable t) {
                    consumer.acceptFailure(t);
                }
                // todo: notify consumer on shutdown?
            }

            private void runImpl(JsonParser parser) throws IOException {
                do {
                    NavContext.processObjectField(parser, path, this);
                } while (multiValue && !parser.hasToken(null));
                // todo: throw exception if not multi value and not null?
                if (count != 0) {
                    flushImpl();
                }
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
                    parser.nextToken();
                }
                parser.nextToken();
            }

            private void processElement(JsonParser parser) throws IOException {
                elementProcessor.processCurrentValue(parser);
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

        // private void loop(JsonParser parser) throws IOException {
        // final Mixin element = element(elementMixin);
        // final List<Type<?>> outputTypes = element.outputTypes().collect(Collectors.toList());
        // // noinspection rawtypes
        // final WritableChunk[] outArrays = new WritableChunk[outputTypes.size()];
        // while (!shutdown) {
        // // TODO: need richer interface w/ unknown size to begin with
        // // todo: blink table vs static
        // final List<WritableChunk<?>> out = newProcessorChunks(chunkSize, outputTypes);
        // final ValueProcessor processor = element.processor("todo", out);
        // int i;
        // for (i = 0; i < chunkSize && isValue(parser.nextToken()); ++i) {
        // processor.processCurrentValue(parser);
        // }
        // if (i == 0) {
        // return;
        // }
        // // noinspection unchecked
        // consumer.accept(out.toArray(outArrays));
        // if (i < chunkSize) {
        // return;
        // }
        // }
        // }
    }

    // private static boolean isValue(JsonToken token) {
    // if (token == null) {
    // return false;
    // }
    // switch (token) {
    // case START_OBJECT:
    // case START_ARRAY:
    // case VALUE_STRING:
    // case VALUE_NUMBER_INT:
    // case VALUE_NUMBER_FLOAT:
    // case VALUE_TRUE:
    // case VALUE_FALSE:
    // case VALUE_NULL:
    // return true;
    // }
    // return false;
    // }

    private static List<WritableChunk<?>> newProcessorChunks(int chunkSize, List<Type<?>> types) {
        return types
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(chunkType -> chunkType.makeWritableChunk(chunkSize))
                .peek(wc -> wc.setSize(0))
                .collect(Collectors.toList());
    }
}
