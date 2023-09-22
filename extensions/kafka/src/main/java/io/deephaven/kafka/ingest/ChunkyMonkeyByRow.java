package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;

final class ChunkyMonkeyByRow<T> extends ChunkyMonkeyRowBased<T> {
    private final ChunkyMonkey1<T> delegate;

    ChunkyMonkeyByRow(ChunkyMonkey1<T> delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public List<Type<?>> types() {
        return delegate.types();
    }

    @Override
    public void splay(T in, List<WritableChunk<?>> out) {
        delegate.splay(in, out);
    }
}
