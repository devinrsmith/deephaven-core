package io.deephaven.chunk;

import java.io.Closeable;
import java.util.Objects;

/**
 * A batched-implementation where
 */
public class ChunksProviderBatch implements ChunksProvider, Closeable {
    private final ChunksProvider handler;
    private Transaction currentTransaction;
    private boolean closed;


    private int committedInRows = 0;
    private int committedOutRows = 0;
    private int openedTxs = 0;
    private int closedTxs = 0;

    public ChunksProviderBatch(ChunksProvider delegate) {
        this.handler = Objects.requireNonNull(delegate);
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


        final Transaction delegate = currentTransaction == null
                ? currentTransaction = handler.tx()
                : currentTransaction;

        return new TransactionBase<Chunks>() {
            @Override
            protected Chunks takeImpl(int minSize) {
                return delegate.take(minSize);
            }

            @Override
            protected void completeImpl(Chunks chunk, int outRows) {
                // don't complete it if there is still space
            }

            @Override
            protected void commitImpl() {

            }

            @Override
            protected void closeImpl(boolean committed, Chunks outstanding, Throwable takeImplThrowable, Throwable completeImplThrowable, Throwable commitImplThrowable) {

            }
        };
        //return new TransactionOnClose(currentTransaction, null);
    }

    // todo: what
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
        closed = true;
    }

    protected boolean shouldCommit(TransactionImpl transaction) {
        return false;
    }

    class MyImpl extends TransactionBase<Chunks> {
        private final Transaction delegate;

        @Override
        protected Chunks takeImpl(int minSize) {
            return delegate.take(minSize);
        }

        @Override
        protected void completeImpl(Chunks chunk, int outRows) {
            delegate.complete(chunk, outRows);
        }

        @Override
        protected void commitImpl() {

        }

        @Override
        protected void closeImpl(boolean committed, Chunks outstanding, Throwable takeImplThrowable, Throwable completeImplThrowable, Throwable commitImplThrowable) {

        }
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
        public void commit() {
            //outstandingInRows += inRows;
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
            delegate.commit();
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
