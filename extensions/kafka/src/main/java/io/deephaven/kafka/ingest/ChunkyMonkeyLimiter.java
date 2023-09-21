package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;

import java.util.List;
import java.util.Objects;

final class ChunkyMonkeyLimiter<T> implements ChunkyMonkey<T> {
    private final ChunkyMonkey<T> delegate;
    private final int maxChunkSize;

    public ChunkyMonkeyLimiter(ChunkyMonkey<T> delegate, int maxChunkSize) {
        if (maxChunkSize <= 0) {
            throw new IllegalArgumentException();
        }
        this.delegate = Objects.requireNonNull(delegate);
        this.maxChunkSize = maxChunkSize;
    }

    @Override
    public List<ChunkType> chunkTypes() {
        return delegate.chunkTypes();
    }

    @Override
    public void handle(T in, List<WritableChunk<?>> out) {
        delegate.handle(in, out);
    }

    @Override
    public void handleChunk(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        final int inSize = in.size();
        for (int i = 0; i < inSize; i += maxChunkSize) {
            delegate.handleChunk(in.slice(i, Math.min(maxChunkSize, inSize - i)), out);
        }
    }
}
