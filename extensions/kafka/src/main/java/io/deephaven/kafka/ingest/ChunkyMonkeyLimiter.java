package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;

import java.util.List;
import java.util.Objects;

final class ChunkyMonkeyLimiter<T> extends ChunkyMonkeyBase<T> {
    private final ChunkyMonkey1<T> delegate;
    private final int maxChunkSize;

    public ChunkyMonkeyLimiter(ChunkyMonkey1<T> delegate, int maxChunkSize) {
        if (maxChunkSize <= 1) {
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
    public void splay(T in, List<WritableChunk<?>> out) {
        delegate.splay(in, out);
    }

    @Override
    public int splay(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        return delegate.splay(in.slice(0, Math.min(maxChunkSize, in.size())), out);
    }
}
