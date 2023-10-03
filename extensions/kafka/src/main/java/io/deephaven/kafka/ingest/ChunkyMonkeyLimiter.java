package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;

final class ChunkyMonkeyLimiter<T> implements ChunkyMonkey1<T> {
    private final ChunkyMonkey1<T> delegate;
    private final int maxChunkSize;

    public ChunkyMonkeyLimiter(ChunkyMonkey1<T> delegate, int maxChunkSize) {
        if (maxChunkSize <= 1) {
            throw new IllegalArgumentException();
        }
        if (delegate.rowLimit() <= maxChunkSize) {
            throw new IllegalArgumentException();
        }
        this.delegate = Objects.requireNonNull(delegate);
        this.maxChunkSize = maxChunkSize;
    }

    ChunkyMonkey1<T> delegate() {
        return delegate;
    }

    @Override
    public List<Type<?>> outputTypes() {
        return delegate.outputTypes();
    }

    @Override
    public int rowLimit() {
        return maxChunkSize;
    }

    @Override
    public void splay(T in, List<WritableChunk<?>> out) {
        delegate.splay(in, out);
    }

    @Override
    public void splay(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        delegate.splay(in.slice(0, Math.min(maxChunkSize, in.size())), out);
    }

    @Override
    public void splayAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        final int inSize = in.size();
        for (int i = 0; i < inSize; i += maxChunkSize) {
            delegate.splay(in.slice(i, Math.min(maxChunkSize, inSize - i)), out);
        }
    }
}
