package io.deephaven.chunk;

import io.deephaven.chunk.MultiChunks.Chunks;
import io.deephaven.chunk.MultiChunks.Handler;
import io.deephaven.chunk.MultiChunks.Transaction;
import io.deephaven.chunk.attributes.Any;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class HandlerSimpleImpl implements Handler {

    private final Consumer<List<Chunks>> consumer;
    private final List<ChunkType> chunkTypes;

    @Override
    public Transaction tx() {
        return new TransactionImpl();
    }

    private class TransactionImpl implements Transaction {
        private final List<Chunks> completed = new ArrayList<>();

        @Override
        public Chunks take(int minSize) {
            return makeChunks(minSize);
        }

        @Override
        public void complete(Chunks chunks, int outRows) {
            completed.add(new ChunksImpl(chunks.out(), outRows));
        }

        @Override
        public void commit(int inRows) {
            consumer.accept(completed);
        }

        @Override
        public void close() {

        }
    }

    private ChunksImpl makeChunks(int minSize) {
        final List<WritableChunk<?>> chunks = chunkTypes.stream()
                .map(c -> c.makeWritableChunk(minSize))
                .collect(Collectors.toList());
        for (WritableChunk<?> chunk : chunks) {
            if (chunk.size() != minSize) {
                throw new IllegalStateException("Expected making chunks to return the exact same size...");
            }
            chunk.setSize(0);
        }
        return new ChunksImpl(chunks, minSize);
    }

    private static class ChunksImpl implements Chunks {
        private final List<WritableChunk<?>> out;
        private final int size;

        private ChunksImpl(List<WritableChunk<?>> out, int size) {
            this.out = List.copyOf(out);
            this.size = size;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public List<WritableChunk<?>> out() {
            return out;
        }
    }
}
