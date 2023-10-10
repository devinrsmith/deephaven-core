package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProvider.Transaction;

import java.util.Objects;

public abstract class TransactionBase<C extends Chunks> implements Transaction {

    private C outstanding;
    private boolean closed;
    private boolean committed;

    private Throwable takeImplThrowable;
    private Throwable completeImplThrowable;
    private Throwable commitImplThrowable;

    @Override
    public final Chunks take(int minSize) {
        if (minSize < 0) {
            throw new IllegalArgumentException("minSize must be non-negative");
        }
        if (closed || committed || takeImplThrowable != null || completeImplThrowable != null || commitImplThrowable != null) {
            throw new IllegalStateException();
        }
        if (outstanding != null) {
            throw new IllegalStateException("Must complete outstanding chunk before taking a new one");
        }
        try {
            return outstanding = Objects.requireNonNull(takeImpl(minSize));
        } catch (Throwable t) {
            takeImplThrowable = t;
            throw t;
        }
    }

    @Override
    public final void complete(Chunks chunks, int outRows) {
        if (chunks == null) {
            throw new NullPointerException("Must not complete null chunks");
        }
        if (outRows < 0) {
            throw new IllegalArgumentException("outRows must be non-negative");
        }
        if (closed || committed || takeImplThrowable != null || completeImplThrowable != null || commitImplThrowable != null) {
            throw new IllegalStateException();
        }
        if (chunks != outstanding) {
            throw new IllegalStateException("Must only complete previously taken chunk");
        }
        try {
            completeImpl(outstanding, outRows);
        } catch (Throwable t) {
            completeImplThrowable = t;
            throw t;
        } finally {
            outstanding = null;
        }
    }

    @Override
    public void commit() {
        if (closed || committed || takeImplThrowable != null || completeImplThrowable != null || commitImplThrowable != null) {
            throw new IllegalStateException();
        }
        if (outstanding != null) {
            throw new IllegalStateException("Outstanding chunk must be completed before committing");
        }
        try {
            commitImpl();
        } catch (Throwable t) {
            commitImplThrowable = t;
            throw t;
        } finally {
            committed = true;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            closeImpl(committed, outstanding, takeImplThrowable, completeImplThrowable, commitImplThrowable);
        } finally {
            outstanding = null;
            takeImplThrowable = null;
            completeImplThrowable = null;
            commitImplThrowable = null;
            closed = true;
        }
    }


    protected abstract C takeImpl(int minSize);

    protected abstract void completeImpl(C chunk, int outRows);

    protected abstract void commitImpl();

    protected abstract void closeImpl(boolean committed, C outstanding, Throwable takeImplThrowable, Throwable completeImplThrowable, Throwable commitImplThrowable);
}
