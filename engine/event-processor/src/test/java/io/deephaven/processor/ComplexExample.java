//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.processor.factory.EventProcessorFactory;
import io.deephaven.processor.factory.EventProcessorFactory.EventProcessor;
import io.deephaven.processor.factory.EventProcessorSpec;
import io.deephaven.processor.factory.EventProcessorStreamSpec;
import io.deephaven.processor.sink.Coordinator;
import io.deephaven.processor.sink.Key;
import io.deephaven.processor.sink.Keys;
import io.deephaven.processor.sink.Sink;
import io.deephaven.processor.sink.Sink.StreamKey;
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
final class ComplexExample {

    private static final Key<Instant> TIMESTAMP = Key.of("timestamp", Type.instantType());
    private static final Key<Long> ID = Key.of("id", Type.longType());

    private static final Key<String> COL_A = Key.of("col_a", Type.stringType());
    private static final Key<Integer> COL_B = Key.of("col_b", Type.intType());

    private static final Key<String> COL_C = Key.of("col_c", Type.stringType());
    private static final Key<Integer> COL_D = Key.of("col_d", Type.intType());

    private static final Key<String> COL_E = Key.of("col_e", Type.stringType());
    private static final Key<Integer> COL_F = Key.of("col_f", Type.intType());

    private static final StreamKey TABLE_1 = new StreamKey();

    private static final StreamKey TABLE_2 = new StreamKey();

    private static final StreamKey TABLE_3 = new StreamKey();


    private static EventProcessorSpec eventSpec(boolean usesCoordinator) {
        return EventProcessorSpec.builder()
                .usesCoordinator(usesCoordinator)
                // table1
                .addStreams(table1Spec())
                // table2
                .addStreams(table2Spec())
                // table3
                .addStreams(table3Spec())
                .build();
    }

    private static EventProcessorStreamSpec table3Spec() {
        return EventProcessorStreamSpec.builder()
                .isRowOriented(true)
                .key(TABLE_1)
                .keys(Keys.builder().addKeys(TIMESTAMP, ID, COL_A, COL_B).build())
                .usesCoordinator(false)
                .build();
    }

    private static EventProcessorStreamSpec table2Spec() {
        return EventProcessorStreamSpec.builder()
                .isRowOriented(false)
                .key(TABLE_2)
                .keys(Keys.builder().addKeys(TIMESTAMP, ID, COL_C, COL_D).build())
                .usesCoordinator(false)
                .build();
    }

    private static EventProcessorStreamSpec table1Spec() {
        return EventProcessorStreamSpec.builder()
                .expectedSize(1)
                .isRowOriented(true)
                .key(TABLE_3)
                .keys(Keys.builder().addKeys(TIMESTAMP, ID, COL_E, COL_F).build())
                .usesCoordinator(false)
                .build();
    }

    private enum EventBased implements EventProcessorFactory<byte[]> {
        EVENT_BASED;

        @Override
        public List<EventProcessorStreamSpec> specs() {
            return List.of(table1Spec(), table2Spec(), table3Spec());
        }

        @Override
        public EventProcessor<byte[]> create(Sink sink) {
            return new EventByEventImpl(sink);
        }
    }

    private enum JsonLinesBased implements EventProcessorFactory<File> {
        JSON_LINES;

        @Override
        public List<EventProcessorStreamSpec> specs() {
            return List.of(table1Spec(), table2Spec(), table3Spec());
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
            this.table1 = new Table1(Sink.get(sink, TABLE_1));
            this.table2 = new Table2(Sink.get(sink, TABLE_2));
            this.table3 = new Table3(Sink.get(sink, TABLE_3));
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
            timestamp = InstantAppender.get(stream, TIMESTAMP);
            id = LongAppender.get(stream, ID);
            colA = ObjectAppender.get(stream, COL_A);
            colB = IntAppender.get(stream, COL_B);
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
            timestamp = InstantAppender.get(stream, TIMESTAMP).asLongEpochAppender(TimeUnit.NANOSECONDS);
            id = LongAppender.get(stream, ID);
            colC = ObjectAppender.get(stream, COL_C);
            colD = IntAppender.get(stream, COL_D);
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

    private static final class Table3 {
        private final Stream stream;
        private final DoubleAppender timestamp;
        private final LongAppender id;
        private final ObjectAppender<String> colE;
        private final IntAppender colF;

        private Table3(Stream stream) {
            this.stream = Objects.requireNonNull(stream);
            timestamp = InstantAppender.get(stream, TIMESTAMP).asDoubleEpochAppender(TimeUnit.SECONDS,
                    RoundingMode.HALF_EVEN);
            id = LongAppender.get(stream, ID);
            colE = ObjectAppender.get(stream, COL_E);
            colF = IntAppender.get(stream, COL_F);
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
