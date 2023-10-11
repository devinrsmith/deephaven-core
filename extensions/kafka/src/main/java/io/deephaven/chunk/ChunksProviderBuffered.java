package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProviderBuffered.MyImpl;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * A batched-implementation where
 */
public class ChunksProviderBuffered extends ChunksProviderBase<MyImpl> {
    private final ChunksProvider handler;
    private Transaction currentDelegate;
    private boolean closed;


    private int committedInRows = 0;
    private int committedOutRows = 0;
    private int openedTxs = 0;
    private int closedTxs = 0;

    public ChunksProviderBuffered(ChunksProvider delegate) {
        this.handler = Objects.requireNonNull(delegate);
    }

    @Override
    protected MyImpl txImpl(Consumer<MyImpl> onClosed) {
        final Transaction delegate = currentDelegate == null
                ? currentDelegate = handler.tx()
                : currentDelegate;
        return new MyImpl(delegate, onClosed);
    }

//    @Override
//    protected void onClosed(MyImpl txn) {
//        super.onClosed(txn);
//        // todo: do other stuff?
//    }

    // todo: what

    // this needs to have threaded support wrt the producer
    public void commit() {
        if (closed) {
            throw new IllegalStateException();
        }
        if (currentDelegate != null) {
            currentDelegate.delegateCommit();
        }
    }


    protected class MyImpl extends TransactionBase<WritableChunks> {
        private final Transaction delegate;
        private final Consumer<MyImpl> onClosed;

        public MyImpl(Transaction delegate, Consumer<MyImpl> onClosed) {
            this.delegate = Objects.requireNonNull(delegate);
            this.onClosed = Objects.requireNonNull(onClosed);
        }

        @Override
        protected WritableChunks takeImpl(int minSize) {
            return delegate.take(minSize);
        }

        @Override
        protected void completeImpl(WritableChunks chunk, int outRows) {
            delegate.complete(chunk, outRows);
        }

        @Override
        protected void commitImpl() {
            // delay commit
        }

        @Override
        protected void closeImpl(boolean committed, WritableChunks outstanding, Throwable takeImplThrowable, Throwable completeImplThrowable, Throwable commitImplThrowable) {
            onClosed.accept(this);
        }
    }
}
