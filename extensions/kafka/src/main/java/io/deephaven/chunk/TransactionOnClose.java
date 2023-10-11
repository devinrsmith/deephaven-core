package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProvider.Transaction;

import java.util.Objects;

public final class TransactionOnClose implements Transaction {
    private final Transaction delegate;
    private final Runnable onClosed;

    public TransactionOnClose(Transaction delegate, Runnable onClosed) {
        this.delegate = Objects.requireNonNull(delegate);
        this.onClosed = Objects.requireNonNull(onClosed);
    }

    @Override
    public WritableChunks take(int minSize) {
        return delegate.take(minSize);
    }

    @Override
    public void complete(WritableChunks chunks, int outRows) {
        delegate.complete(chunks, outRows);
    }

    @Override
    public void commit() {
        delegate.commit();
    }

    @Override
    public void close() {
        try {
            delegate.close();
        } finally {
            onClosed.run();
        }
    }
}
