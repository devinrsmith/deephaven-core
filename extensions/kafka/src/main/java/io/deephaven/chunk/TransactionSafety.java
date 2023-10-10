package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProducer.Transaction;

import java.util.Objects;

/**
 * Adds basic safety checks around a {@code delegate} implementation. Does <b>not</b> add thread-safety.
 */
public final class TransactionSafety implements Transaction {
    private final Transaction delegate;
    private final Runnable onClose;
    private Chunks outstanding;
    private boolean committed;
    private boolean closed;

    public TransactionSafety(Transaction delegate, Runnable onClose) {
        this.delegate = Objects.requireNonNull(delegate);
        this.onClose = onClose;
    }

    @Override
    public Chunks take(int minSize) {
        if (minSize < 0) {
            throw new IllegalArgumentException("minSize must be non-negative");
        }
        if (closed || committed) {
            throw new IllegalStateException();
        }
        if (outstanding != null) {
            throw new IllegalStateException("Must complete outstanding chunk before taking a new one");
        }
        return outstanding = Objects.requireNonNull(delegate.take(minSize));
    }

    @Override
    public void complete(Chunks chunks, int outRows) {
        if (chunks == null) {
            throw new NullPointerException("Must not complete null chunks");
        }
        if (outRows < 0) {
            throw new IllegalArgumentException("outRows must be non-negative");
        }
        if (closed || committed) {
            throw new IllegalStateException();
        }
        if (chunks != outstanding) {
            throw new IllegalStateException("Must only complete previously taken chunk");
        }
        delegate.complete(chunks, outRows);
        outstanding = null;
    }

    @Override
    public void commit(int inRows) {
        if (inRows < 0) {
            throw new IllegalArgumentException("inRows must be non-negative");
        }
        if (closed || committed) {
            throw new IllegalStateException();
        }
        if (outstanding != null) {
            throw new IllegalStateException("Outstanding chunk must be completed before committing");
        }
        delegate.commit(inRows);
        committed = true;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        outstanding = null;
        delegate.close();
        if (onClose != null) {
            onClose.run();
        }
        closed = true;
    }
}
