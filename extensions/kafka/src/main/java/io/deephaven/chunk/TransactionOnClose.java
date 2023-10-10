package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProducer.Transaction;

import java.util.Objects;

public class TransactionOnClose implements Transaction {
    private final Transaction delegate;
    private final Runnable onClose;


    public TransactionOnClose(Transaction delegate, Runnable onClose) {
        this.delegate = Objects.requireNonNull(delegate);
        this.onClose = Objects.requireNonNull(onClose);
    }

    @Override
    public Chunks take(int minSize) {
        return delegate.take(minSize);
    }

    @Override
    public void complete(Chunks chunks, int outRows) {
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
            onClose.run();
        }
    }
}
