package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProvider.Transaction;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.stream.StreamConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class StreamConsumerTx implements Transaction {

    private final StreamConsumer consumer; // todo: should this just be a normal consumer?

    private final List<ChunksImpl> chunks;

    private final int chunkSize;

    public StreamConsumerTx(StreamConsumer consumer, int chunkSize) {
        this.consumer = Objects.requireNonNull(consumer);
        this.chunks = new ArrayList<>();
        this.chunkSize = chunkSize;
    }

    @Override
    public WritableChunks take(int minSize) {
        if (chunks.isEmpty()) {
            return newChunk(minSize);
        }
        final ChunksImpl current = chunks.get(chunks.size() - 1);
        return current.size() >= minSize ? current : newChunk(minSize);
    }

    @Override
    public void complete(WritableChunks chunks, int outRows) {
        ((ChunksImpl) chunks).complete(outRows);
    }

    @Override
    public void submit() {
        consumer.accept(chunks.stream().map(c -> c.out).collect(Collectors.toList()));
    }

    @Override
    public void close() {
        // todo, pass failure to consumer if not committed?
    }

    private ChunksImpl newChunk(int minSize) {
        // todo
        final ChunksImpl next = new ChunksImpl(Math.max(minSize, chunkSize), null);
        chunks.add(next);
        return next;
    }

    private static class ChunksImpl implements WritableChunks {

        private final int chunkSize;
        private final WritableChunk<Values>[] out;
        private int used;

        public ChunksImpl(int chunkSize, WritableChunk<Values>[] out) {
            this.chunkSize = chunkSize;
            this.out = Objects.requireNonNull(out);
            this.used = 0;
        }

        @Override
        public int size() {
            return chunkSize - used;
        }

        @Override
        public List<WritableChunk<?>> out() {
            return Arrays.asList(out);
        }

        void complete(int outRows) {
            used += outRows;
        }
    }
}
