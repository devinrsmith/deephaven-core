//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.processor.factory.EventProcessorFactory;
import io.deephaven.processor.factory.EventProcessorFactory.EventProcessor;
import io.deephaven.processor.factory.EventProcessorSpec;
import io.deephaven.processor.factory.EventProcessorStreamSpec;
import io.deephaven.processor.sink.Coordinator;
import io.deephaven.processor.sink.Sink;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.DoubleAppender;
import io.deephaven.processor.sink.appender.InstantAppender;
import io.deephaven.processor.sink.appender.IntAppender;
import io.deephaven.processor.sink.appender.LongAppender;
import io.deephaven.processor.sink.appender.ObjectAppender;
import io.deephaven.qst.type.Type;
import io.deephaven.time.DateTimeUtils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

// TODO: let's say we have JSONL of these, how does impl change (yes to coordinator?)
/*
 * { "timestamp": "...", "id": "...", "table1": { "colA": "Hi", "colB": 42 }, "table2": { "colC": [ "Hi", ... ], "colD":
 * [ 42, ... ] }, "table3": [ { "colE": "Hi", "colF": 42 }, ... ] }
 */
final class ComplexExample2 {
    private static EventProcessorSpec eventSpec(boolean usesCoordinator) {
        return EventProcessorSpec.builder()
                .usesCoordinator(usesCoordinator)
                // table1
                .addStreams(EventProcessorStreamSpec.builder()
                        .expectedSize(1)
                        .isRowOriented(true)
                        .addOutputTypes(Type.instantType())
                        .addOutputTypes(Type.longType())
                        .addOutputTypes(Type.stringType())
                        .addOutputTypes(Type.intType())
                        .build())
                // table2
                .addStreams(EventProcessorStreamSpec.builder()
                        .isRowOriented(false)
                        .addOutputTypes(Type.instantType())
                        .addOutputTypes(Type.longType())
                        .addOutputTypes(Type.stringType())
                        .addOutputTypes(Type.intType())
                        .build())
                // table3
                .addStreams(EventProcessorStreamSpec.builder()
                        .isRowOriented(true)
                        .addOutputTypes(Type.instantType())
                        .addOutputTypes(Type.longType())
                        .addOutputTypes(Type.stringType())
                        .addOutputTypes(Type.intType())
                        .build())
                .build();
    }

    private enum EventBased implements EventProcessorFactory<byte[]> {
        EVENT_BASED;

        @Override
        public EventProcessorSpec spec() {
            return eventSpec(false);
        }

        @Override
        public EventProcessor<byte[]> create(Sink sink) {
            return new EventByEventImpl(sink);
        }
    }

    private enum JsonLinesBased implements EventProcessorFactory<File> {
        JSON_LINES;

        @Override
        public EventProcessorSpec spec() {
            return eventSpec(true);
        }

        @Override
        public EventProcessor<File> create(Sink sink) {
            return new JsonLinesImpl(sink);
        }
    }

    public static EventProcessorFactory<byte[]> single() {
        return EventBased.EVENT_BASED;
    }

    public static EventProcessorFactory<File> jsonLines() {
        return JsonLinesBased.JSON_LINES;
    }

    private static final class EventByEventImpl extends IOEventProcessor<byte[]> {
        private final Table123 table123;

        public EventByEventImpl(Sink sink) {
            this.table123 = new Table123(sink);
        }

        @Override
        public void acceptImpl(byte[] event) throws IOException {
            try (final JsonParser parser = new JsonFactory().createParser(event)) {
                parser.nextToken();
                table123.process(parser);
                if (parser.nextToken() != null) {
                    throw new IllegalStateException();
                }
            }
        }

        @Override
        public void close() {

        }
    }

    private static final class JsonLinesImpl extends IOEventProcessor<File> {
        private final Coordinator coordinator;
        private final Table123 table123;

        public JsonLinesImpl(Sink sink) {
            this.coordinator = sink.coordinator();
            this.table123 = new Table123(sink);
        }

        @Override
        public void acceptImpl(File file) throws IOException {
            try (final JsonParser parser = new JsonFactory().createParser(file)) {
                while (parser.nextToken() != null) {
                    // semantics match up better; should be OK to deliver empty at first
                    coordinator.yield();
                    table123.process(parser);
                }
            }
        }

        @Override
        public void close() {

        }
    }

    private static abstract class IOEventProcessor<T> implements EventProcessor<T> {

        abstract void acceptImpl(T event) throws IOException;

        @Override
        public void writeToSink(T event) {
            try {
                acceptImpl(event);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static final class Table123 {
        private final Table1 table1;
        private final Table2 table2;
        private final Table3 table3;

        private Table123(Sink sink) {
            if (sink.streams().size() != 3) {
                throw new IllegalArgumentException();
            }
            this.table1 = new Table1(sink.streams().get(0));
            this.table2 = new Table2(sink.streams().get(1));
            this.table3 = new Table3(sink.streams().get(2));
        }

        public void process(JsonParser parser) throws IOException {
            {
                startObject(parser);
                parser.nextToken();
            }
            final Instant timestamp;
            {
                fieldName(parser, "timestamp");
                parser.nextToken();
                timestamp = Instant.parse(stringValue(parser));
                parser.nextToken();
            }
            final long id;
            {
                fieldName(parser, "id");
                parser.nextToken();
                id = longValue(parser);
                parser.nextToken();
            }
            {
                fieldName(parser, "table1");
                parser.nextToken();
                table1.processOne(timestamp, id, parser);
                parser.nextToken();
            }
            {
                fieldName(parser, "table2");
                parser.nextToken();
                table2.processMany(timestamp, id, parser);
                parser.nextToken();
            }
            {
                fieldName(parser, "table3");
                parser.nextToken();
                table3.processMany(timestamp, id, parser);
                parser.nextToken();
            }
            {
                endObject(parser);
                // not advancing past end, callers job
            }
        }
    }

    private static final class Table1 {

        private final Stream stream;
        private final InstantAppender timestamp;
        private final LongAppender id;
        private final ObjectAppender<String> colA;
        private final IntAppender colB;

        private Table1(Stream stream) {
            this.stream = Objects.requireNonNull(stream);
            if (stream.appenders().size() != 4) {
                throw new IllegalArgumentException();
            }
            timestamp = InstantAppender.get(stream.appenders().get(0));
            id = LongAppender.get(stream.appenders().get(1));
            colA = ObjectAppender.get(stream.appenders().get(2), Type.stringType());
            colB = IntAppender.get(stream.appenders().get(3));
        }

        public void processOne(Instant timestamp, long id, JsonParser parser) throws IOException {
            stream.ensureRemainingCapacity(1);
            {
                startObject(parser);
                parser.nextToken();
            }
            {
                fieldName(parser, "colA");
                parser.nextToken();
                colA.set(stringValue(parser));
                parser.nextToken();
            }
            {
                fieldName(parser, "colB");
                parser.nextToken();
                colB.set(intValue(parser));
                parser.nextToken();
            }
            {
                endObject(parser);
                // not advancing past end, callers job
            }
            this.timestamp.set(timestamp);
            this.id.set(id);
            stream.advanceAll();
        }
    }

    private static final class Table2 {
        private final Stream stream;
        private final LongAppender timestamp;
        private final LongAppender id;
        private final ObjectAppender<String> colC;
        private final IntAppender colD;

        private Table2(Stream stream) {
            this.stream = Objects.requireNonNull(stream);
            if (stream.appenders().size() != 4) {
                throw new IllegalArgumentException();
            }
            timestamp = InstantAppender.get(stream.appenders().get(0)).asLongEpochAppender(TimeUnit.NANOSECONDS);
            id = LongAppender.get(stream.appenders().get(1));
            colC = ObjectAppender.get(stream.appenders().get(2), Type.stringType());
            colD = IntAppender.get(stream.appenders().get(3));
        }

        public void processMany(Instant timestamp, long id, JsonParser parser) throws IOException {
            stream.ensureRemainingCapacity(2);
            {
                startObject(parser);
                parser.nextToken();
            }
            final int size;
            {
                fieldName(parser, "colC");
                parser.nextToken();
                startArray(parser);
                int i = 0;
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    ObjectAppender.append(colC, stringValue(parser));
                    ++i;
                }
                parser.nextToken();
                size = i;
            }
            {
                fieldName(parser, "colD");
                parser.nextToken();
                startArray(parser);
                parser.nextToken();
                for (int i = 0; i < size; ++i) {
                    IntAppender.append(colD, intValue(parser));
                    parser.nextToken();
                }
                endArray(parser);
                parser.nextToken();
            }
            {
                endObject(parser);
                // not advancing past end, callers job
            }
            // fill in based on size. TODO: fill helpers?
            for (int i = 0; i < size; ++i) {
                LongAppender.append(this.timestamp, DateTimeUtils.epochNanos(timestamp));
            }
            for (int i = 0; i < size; ++i) {
                LongAppender.append(this.id, id);
            }
        }
    }

    interface SomeSupplier {
        List<WritableChunk<?>> swap(List<WritableChunk<?>> done, int pos);
    }

    private static final class Table3 {
        private final Stream stream;
        private final DoubleAppender timestamp;
        private final LongAppender id;
        private final ObjectAppender<String> colE;
        private final IntAppender colF;

        private final SomeSupplier someSupplier;
        private int pos;
        private WritableDoubleChunk<?> timestampChunk;
        private WritableLongChunk<?> idChunk;
        private WritableObjectChunk<String, ?> colEChunk;
        private WritableIntChunk<?> colFChunk;

        private Table3(Stream stream) {
            this.stream = Objects.requireNonNull(stream);
            if (stream.appenders().size() != 4) {
                throw new IllegalArgumentException();
            }
            timestamp = InstantAppender.get(stream.appenders().get(0)).asDoubleEpochAppender(TimeUnit.SECONDS,
                    RoundingMode.HALF_EVEN);
            id = LongAppender.get(stream.appenders().get(1));
            colE = ObjectAppender.get(stream.appenders().get(2), Type.stringType());
            colF = IntAppender.get(stream.appenders().get(3));
            someSupplier = null; // todo
        }

        public void flush() {
            timestampChunk.setSize(pos);
            final List<WritableChunk<?>> newChunks =
                    someSupplier.swap(List.of(timestampChunk, idChunk, colEChunk, colFChunk), pos);
            pos = 0;
            timestampChunk = (WritableDoubleChunk<?>) newChunks.get(0);

            //
        }

        public void processMany2(Instant timestamp, long id, JsonParser parser) throws IOException {
            stream.ensureRemainingCapacity(3);
            startArray(parser);
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                {
                    startObject(parser);
                    parser.nextToken();
                }
                {
                    fieldName(parser, "colE");
                    parser.nextToken();
                    colEChunk.set(pos, stringValue(parser));
                    parser.nextToken();
                }
                {
                    fieldName(parser, "colF");
                    parser.nextToken();
                    colFChunk.set(pos, intValue(parser));
                    parser.nextToken();
                }
                {
                    endObject(parser);
                }
                this.timestampChunk.set(pos, DateTimeUtils.epochNanos(timestamp) / 1_000_000_000.0);
                this.idChunk.set(pos, id);
                ++pos;

                if (pos == 1024) {
                    flush();
                }
            }
            // not advancing past end, callers job
        }

        public void processMany(Instant timestamp, long id, JsonParser parser) throws IOException {
            stream.ensureRemainingCapacity(3);
            startArray(parser);
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                {
                    startObject(parser);
                    parser.nextToken();
                }
                {
                    fieldName(parser, "colE");
                    parser.nextToken();
                    colE.set(stringValue(parser));
                    parser.nextToken();
                }
                {
                    fieldName(parser, "colF");
                    parser.nextToken();
                    colF.set(intValue(parser));
                    parser.nextToken();
                }
                {
                    endObject(parser);
                }
                this.timestamp.set(DateTimeUtils.epochNanos(timestamp) / 1_000_000_000.0);
                this.id.set(id);
                stream.advanceAll();
            }
            // not advancing past end, callers job
        }
    }

    private static void startObject(JsonParser parser) {
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            throw new IllegalStateException();
        }
    }

    private static void endObject(JsonParser parser) {
        if (parser.currentToken() != JsonToken.END_OBJECT) {
            throw new IllegalStateException();
        }
    }

    private static void startArray(JsonParser parser) {
        if (parser.currentToken() != JsonToken.START_ARRAY) {
            throw new IllegalStateException();
        }
    }

    private static void endArray(JsonParser parser) {
        if (parser.currentToken() != JsonToken.END_ARRAY) {
            throw new IllegalStateException();
        }
    }

    private static void fieldName(JsonParser parser, String fieldName) throws IOException {
        if (parser.currentToken() != JsonToken.FIELD_NAME) {
            throw new IllegalStateException();
        }
        if (!fieldName.equals(parser.currentName())) {
            throw new IllegalStateException();
        }
    }

    private static String stringValue(JsonParser parser) throws IOException {
        if (parser.currentToken() != JsonToken.VALUE_STRING) {
            throw new IllegalStateException();
        }
        return parser.getText();
    }

    private static int intValue(JsonParser parser) throws IOException {
        if (parser.currentToken() != JsonToken.VALUE_NUMBER_INT) {
            throw new IllegalStateException();
        }
        return parser.getIntValue();
    }

    private static long longValue(JsonParser parser) throws IOException {
        if (parser.currentToken() != JsonToken.VALUE_NUMBER_INT) {
            throw new IllegalStateException();
        }
        return parser.getLongValue();
    }
}
