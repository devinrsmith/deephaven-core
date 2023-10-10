package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProvider.Transaction;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;

class ObjectChunksOneToManyRowLimited<T> implements ObjectChunksOneToMany<T> {
    private final ObjectChunksOneToOne<T> impl;
    private final int txSize;

    public ObjectChunksOneToManyRowLimited(ObjectChunksOneToOne<T> impl, int maxChunkSize) {
        this.impl = Objects.requireNonNull(impl);
        this.txSize = maxChunkSize;
    }

    @Override
    public List<Type<?>> outputTypes() {
        return impl.outputTypes();
    }

    @Override
    public void handleAll(ObjectChunk<? extends T, ?> in, ChunksProvider out) {
        for (final ObjectChunk<? extends T, ?> slice : ObjectChunksOneToOneRowLimitedImpl.iterable(in, txSize)) {
            try (final Transaction tx = out.tx()) {
                final int sliceSize = slice.size();
                final Chunks chunks = tx.take(sliceSize);
                impl.splayAll(slice, chunks.out());
                tx.complete(chunks, sliceSize);
                tx.commit();
            }
        }
    }
}
