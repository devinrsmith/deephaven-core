package io.deephaven.chunk;

import java.io.Closeable;
import java.util.Objects;

public final class HandlerSafety implements ChunksProvider, Closeable {
    private final ChunksProvider delegate;
    private Transaction outstanding;
    private boolean closed;

    public HandlerSafety(ChunksProvider delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public Transaction tx() {
        if (closed) {
            throw new IllegalStateException();
        }
        if (outstanding != null) {
            throw new IllegalStateException("Must close existing transaction before getting new one");
        }
        return outstanding = new TransactionSafety(delegate.tx(), this::clearOutstanding);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        if (outstanding != null) {
            outstanding.close();
        }
        closed = true;
    }

    private void clearOutstanding() {
        outstanding = null;
    }
}
