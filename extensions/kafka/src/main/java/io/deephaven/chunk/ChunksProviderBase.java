package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProvider.Transaction;

import java.io.Closeable;
import java.util.Objects;
import java.util.function.Consumer;

public abstract class ChunksProviderBase<T extends Transaction> implements ChunksProvider, Closeable {

    private T outstanding;
    private boolean closed;

    @Override
    public Transaction tx() {
        if (closed) {
            throw new IllegalStateException();
        }
        if (outstanding != null) {
            throw new IllegalStateException("Must close existing transaction");
        }
        return outstanding = Objects.requireNonNull(txImpl(this::onClosed));
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (outstanding != null) {
            outstanding.close();
            outstanding = null;
        }
    }

    protected abstract T txImpl(Consumer<T> onClosed);

    protected void onClosed(T txn) {
        if (outstanding != txn) {
            throw new IllegalStateException();
        }
        outstanding = null;
    }
}
