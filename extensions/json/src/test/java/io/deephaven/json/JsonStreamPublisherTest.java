/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkEquals;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonStreamPublisherTest {
    @Test
    void singlePrimitive() {
        final JsonStreamPublisher publisher = publisher(IntOptions.standard(), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            executeInline(publisher, Source.of("42"), Source.of("null"), Source.of("43"));
            executeInline(publisher, Source.of("1"), Source.of("-2"));
            recorder.flushPublisher();
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[]{42, QueryConstants.NULL_INT, 43}));
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[]{1, -2}));
            recorder.assertEmpty();
            recorder.clear();
        }
    }

    @Test
    void multiPrimitive() {
        final JsonStreamPublisher publisher = publisher(IntOptions.standard(), true);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            executeInline(publisher, Source.of("42 null 43"));
            executeInline(publisher, Source.of("1\n\n-2"));
            recorder.flushPublisher();
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[]{42, QueryConstants.NULL_INT, 43}));
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[]{1, -2}));
            recorder.assertEmpty();
            recorder.clear();
        }
    }

    @Test
    void singlePrimitiveArray() {
        final JsonStreamPublisher publisher = publisher(ArrayOptions.strict(IntOptions.standard()), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            executeInline(publisher, Source.of("[42, null, 43]"));
            executeInline(publisher, Source.of("[1, -2]"));
            recorder.flushPublisher();
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[]{42, QueryConstants.NULL_INT, 43}));
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[]{1, -2}));
            recorder.assertEmpty();
            recorder.clear();
        }
    }

    @Test
    void multiPrimitiveArray() {
        final JsonStreamPublisher publisher = publisher(ArrayOptions.strict(IntOptions.standard()), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            executeInline(publisher, Source.of("[42, null, 43]"));
            executeInline(publisher, Source.of("[1, -2]"));
            recorder.flushPublisher();
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[]{42, QueryConstants.NULL_INT, 43}));
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[]{1, -2}));
            recorder.assertEmpty();
            recorder.clear();
        }
    }

    private static JsonStreamPublisher publisher(ValueOptions options, boolean multiValueSupport) {
        return JsonStreamPublisherOptions.builder()
                .options(options)
                .multiValueSupport(multiValueSupport)
                .chunkSize(1024)
                .build()
                .execute();
    }

    private static void executeInline(JsonStreamPublisher publisher, Source... sources) {
        publisher.execute(Runnable::run, Arrays.asList(sources));
    }

    // todo: consolidate between this and io.deephaven.kafka.v2.PublishersOptionsBase.StreamConsumerRecorder
    private static class StreamConsumerRecorder implements StreamConsumer, Closeable {
        final StreamPublisher publisher;
        final List<WritableChunk<Values>[]> accepted = new ArrayList<>();
        final List<Throwable> failures = new ArrayList<>();
        // private final LongAdder rowCount = new LongAdder();

        private StreamConsumerRecorder(StreamPublisher publisher) {
            this.publisher = Objects.requireNonNull(publisher);
        }

        public void flushPublisher() {
            publisher.flush();
        }

        public synchronized void clear() {
            if (!failures.isEmpty()) {
                throw new IllegalStateException();
            }
            accepted.clear();
        }

        @Override
        public synchronized void accept(@NotNull WritableChunk<Values>... data) {
            // rowCount.add(data[0].size());
            accepted.add(data);
        }

        @Override
        public synchronized void accept(@NotNull Collection<WritableChunk<Values>[]> data) {
            // rowCount.add(data.stream().map(x -> x[0]).mapToInt(Chunk::size).sum());
            accepted.addAll(data);
        }

        @Override
        public synchronized void acceptFailure(@NotNull Throwable cause) {
            failures.add(cause);
        }

        @Override
        public void close() {
            publisher.shutdown();
        }

        public synchronized Chunk<?> singleValue() {
            assertThat(failures).isEmpty();
            assertThat(accepted).hasSize(1);
            assertThat(accepted.get(0)).hasSize(1);
            return accepted.get(0)[0];
        }

        public synchronized void assertEquals(Chunk<?>... expectedChunks) {
            assertThat(failures).isEmpty();
            assertThat(accepted).hasSize(1);
            assertNextChunksEquals(expectedChunks);
        }

        public synchronized void assertEmpty() {
            assertThat(failures).isEmpty();
            assertThat(accepted).isEmpty();
        }

        public synchronized void assertNextChunksEquals(Chunk<?>... expectedChunks) {
            assertThat(failures).isEmpty();
            final Iterator<WritableChunk<Values>[]> it = accepted.iterator();
            assertThat(it).hasNext();
            final WritableChunk<Values>[] chunks = it.next();
            assertThat(chunks).hasSize(expectedChunks.length);
            for (int i = 0; i < chunks.length; ++i) {
                assertThat(chunks[i]).usingComparator(FAKE_COMPARE_FOR_EQUALS).isEqualTo(expectedChunks[i]);
            }
            it.remove();
        }
    }

    private static final Comparator<Chunk<?>> FAKE_COMPARE_FOR_EQUALS =
            JsonStreamPublisherTest::fakeCompareForEquals;

    private static int fakeCompareForEquals(Chunk<?> x, Chunk<?> y) {
        return ChunkEquals.equals(x, y) ? 0 : 1;
    }
}
