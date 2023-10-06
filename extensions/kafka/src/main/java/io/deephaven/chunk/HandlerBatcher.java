package io.deephaven.chunk;

import io.deephaven.chunk.MultiChunks.Chunks;
import io.deephaven.chunk.MultiChunks.Handler;
import io.deephaven.chunk.MultiChunks.Transaction;

import java.util.Objects;

/**
 * A batched-implementation where
 */
public final class HandlerBatcher implements Handler {
    private final Transaction delegate;
    private int outstandingInRows = 0;
    private int outstandingOutRows = 0;
    private int committedInRows = 0;
    private int committedOutRows = 0;
    private int openedTxs = 0;
    private int closedTxs = 0;

    public HandlerBatcher(Transaction delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    public void commitOutstanding() {
        delegate.commit(outstandingInRows);
        committedInRows += outstandingInRows;
        committedOutRows += outstandingOutRows;
        outstandingInRows = 0;
        outstandingOutRows = 0;
    }

    public int outstandingInRows() {
        return outstandingInRows;
    }

    public int outstandingOutRows() {
        return outstandingOutRows;
    }

    public int committedInRows() {
        return committedInRows;
    }

    public int committedOutRows() {
        return committedOutRows;
    }

    @Override
    public Transaction tx() {
        if (openedTxs != closedTxs) {
            throw new IllegalStateException();
        }
        openedTxs++;
        return new TransactionSafety(new TransactionImpl());
    }

    private class TransactionImpl implements Transaction {

        @Override
        public Chunks take(int minSize) {
            return delegate.take(minSize);
        }

        @Override
        public void complete(Chunks chunks, int outRows) {
            delegate.complete(chunks, outRows);
            outstandingOutRows += outRows;
        }

        @Override
        public void commit(int inRows) {
            outstandingInRows += inRows;
        }

        @Override
        public void close() {
            ++closedTxs;
        }
    }
}
