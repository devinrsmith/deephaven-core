package io.deephaven.chunk;

import io.deephaven.chunk.MultiChunks.Chunks;
import io.deephaven.chunk.MultiChunks.Transaction;

import java.util.Objects;

/**
 * Adds basic safety checks around a {@code delegate} implementation. Does <b>not</b> add thread-safety.
 */
public final class TransactionSafety implements Transaction {
    private final Transaction delegate;
    private Chunks outstanding;
    private boolean closed;

    public TransactionSafety(Transaction delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public Chunks take(int minSize) {
        if (closed) {
            throw new IllegalStateException();
        }
        if (outstanding != null) {
            throw new IllegalStateException();
        }
        return delegate.take(minSize);
    }

    @Override
    public void complete(Chunks chunks, int outRows) {
        if (closed) {
            throw new IllegalStateException();
        }
        if (chunks != outstanding) {
            throw new IllegalStateException();
        }
        delegate.complete(chunks, outRows);
        outstanding = null;
    }

    @Override
    public void commit(int inRows) {
        if (closed) {
            throw new IllegalStateException();
        }
        delegate.commit(inRows);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        delegate.close();
        outstanding = null;
        closed = true;
    }
}
