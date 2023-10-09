package io.deephaven.chunk;

import io.deephaven.chunk.MultiChunks.Chunks;
import io.deephaven.chunk.MultiChunks.Handler;
import io.deephaven.chunk.MultiChunks.Transaction;

import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.util.Objects;

/**
 * A batched-implementation where
 */
public class HandlerBatcher implements Handler, Closeable {
    private final Handler handler;
    private TransactionImpl currentTransaction;
    private boolean closed;


    private int committedInRows = 0;
    private int committedOutRows = 0;
    private int openedTxs = 0;
    private int closedTxs = 0;

    public HandlerBatcher(Handler handler) {
        this.handler = Objects.requireNonNull(handler);
    }


    @Override
    public Transaction tx() {
        if (closed) {
            throw new IllegalStateException();
        }
        if (openedTxs != closedTxs) {
            throw new IllegalStateException();
        }
        openedTxs++;
        if (currentTransaction == null) {
            currentTransaction = new TransactionImpl(handler.tx());
        }
        return new TransactionSafety(currentTransaction);
    }

    // thread safe, may be called from multiple threads
    public void commit() {
        if (closed) {
            throw new IllegalStateException();
        }
        if (currentTransaction != null) {
            currentTransaction.delegateCommit();
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        if (currentTransaction != null) {
            currentTransaction.delegateClose();
        }
    }

    protected boolean shouldCommit(TransactionImpl transaction) {
        return false;
    }

    protected final class TransactionImpl implements Transaction {
        private final Transaction delegate;
        private int completedOutRows;
        private int outstandingInRows;
        private int outstandingOutRows;
        private boolean committed;

        private TransactionImpl(Transaction delegate) {
            this.delegate = Objects.requireNonNull(delegate);
            this.outstandingInRows = 0;
            this.completedOutRows = 0;
            this.committed = false;
        }

        public int outstandingInRows() {
            return outstandingInRows;
        }

        public int outstandingOutRows() {
            return completedOutRows;
        }

        @Override
        public Chunks take(int minSize) {
            return delegate.take(minSize);
        }

        @Override
        public void complete(Chunks chunks, int outRows) {
            delegate.complete(chunks, outRows);
            completedOutRows += outRows;
        }

        @Override
        public void commit(int inRows) {
            outstandingInRows += inRows;
            outstandingOutRows += completedOutRows;
            completedOutRows = 0;
            if (shouldCommit(this)) {
                delegateCommit();
            }
        }

        @Override
        public void close() {
            ++closedTxs;
            if (committed) {
                delegateClose();
            }
        }

        private void delegateCommit() {
            delegate.commit(outstandingInRows);
            committedInRows += outstandingInRows;
            committedOutRows += completedOutRows;
            committed = true;
        }

        private void delegateClose() {
            delegate.close();
            currentTransaction = null;
        }
    }
}
