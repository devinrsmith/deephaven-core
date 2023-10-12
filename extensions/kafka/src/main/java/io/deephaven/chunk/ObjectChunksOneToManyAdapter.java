package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProvider.Transaction;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;

import static io.deephaven.chunk.ObjectChunksOneToOneRowLimitedImpl.iterable;

class ObjectChunksOneToManyAdapter<T> implements ObjectChunksOneToMany<T> {
    private final ObjectChunksOneToOne<T> delegate;
    private final int maxTakeSize;
    private final int maxTransactionSize;

    ObjectChunksOneToManyAdapter(ObjectChunksOneToOne<T> delegate, int maxTxSize, int maxTakeSize) {
        if (maxTakeSize <= 0 || maxTakeSize > maxTxSize) {
            throw new IllegalArgumentException(String.format(
                    "Must have 0 < maxTakeSize <= maxTxSize. maxTakeSize=%d, maxTxSize=%d",
                    maxTakeSize, maxTxSize));
        }
        // This is not _technically_ a limitation, but it's best practice for chunk-sizing sympathy.
        if (maxTxSize % maxTakeSize != 0) {
            throw new IllegalArgumentException(String.format(
                    "Must have maxTxSize %% maxTakeSize == 0. maxTxSize=%d, maxTakeSize=%d",
                    maxTxSize, maxTakeSize));
        }
        this.delegate = Objects.requireNonNull(delegate);
        this.maxTakeSize = maxTakeSize;
        this.maxTransactionSize = maxTxSize;
    }

    @Override
    public List<Type<?>> outputTypes() {
        return delegate.outputTypes();
    }

    @Override
    public void handleAll(List<? extends ObjectChunk<? extends T, ?>> inChunks, ChunksProvider out) {
        // todo: should txSize be part of Transaction?
        int txSize = 0;
        Transaction tx = out.tx();
        try {
            for (final ObjectChunk<? extends T, ?> in : inChunks) {
                for (final ObjectChunk<? extends T, ?> tookSlice : iterable(in, maxTakeSize)) {
                    for (final ObjectChunk<? extends T, ?> pip : split(tookSlice, maxTransactionSize - txSize)) {
                        // We know the first item will have size <= remaining. If there _is_ a second item, we know it
                        // will be with respect to a _fresh_ transaction; and since we know
                        // maxTakeSize <= maxTransactionSize, we know its size will be <= maxTransactionSize.
                        final int pipSize = pip.size();
                        final WritableChunks chunks = tx.take(pipSize);
                        delegate.splayAll(pip, chunks.out());
                        tx.complete(chunks, pipSize);
                        txSize += pipSize;
                        if (txSize >= maxTransactionSize) {
                            if (txSize > maxTransactionSize) {
                                throw new IllegalStateException();
                            }
                            try (final Transaction localTx = tx) {
                                tx = null;
                                txSize = 0;
                                localTx.submit();
                            }
                            tx = out.tx();
                        }
                    }
                }
            }
        } finally {
            if (tx != null) {
                tx.close();
            }
        }
    }

    static <T, ATTR extends Any> Iterable<ObjectChunk<T, ATTR>> split(ObjectChunk<T, ATTR> source, int pos) {
        return source.size() <= pos
                ? List.of(source)
                : List.of(source.slice(0, pos), source.slice(pos, source.size() - pos));
    }
}
