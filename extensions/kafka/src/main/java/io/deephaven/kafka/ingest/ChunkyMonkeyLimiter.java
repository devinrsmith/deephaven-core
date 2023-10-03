package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;

final class ChunkyMonkeyLimiter<T> implements ChunkyMonkeyRowLimited<T> {
    private final ChunkyMonkey<T> delegate;
    private final int maxChunkSize;

    ChunkyMonkeyLimiter(ChunkyMonkey<T> delegate, int maxChunkSize) {
        if (maxChunkSize <= 0) {
            throw new IllegalArgumentException("maxChunkSize must be positive");
        }
        if (maxChunkSize == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("maxChunkSize must be less than Integer.MAX_VALUE");
        }
        this.delegate = Objects.requireNonNull(delegate);
        this.maxChunkSize = maxChunkSize;
    }

    ChunkyMonkey<T> delegate() {
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
    public void splayAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        final int inSize = in.size();
        for (int i = 0; i < inSize; i += maxChunkSize) {
            delegate.splayAll(in.slice(i, Math.min(maxChunkSize, inSize - i)), out);
        }
    }
}
