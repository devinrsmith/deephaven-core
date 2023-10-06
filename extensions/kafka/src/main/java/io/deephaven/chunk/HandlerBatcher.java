package io.deephaven.chunk;

import io.deephaven.chunk.MultiChunks.Chunks;
import io.deephaven.chunk.MultiChunks.Handler;
import io.deephaven.chunk.MultiChunks.Transaction;

import java.util.Objects;

public class HandlerBatcher implements Handler {
    private final Transaction rootTx;
    private final TransactionImpl txImpl;
    private int inRows = 0;
    private int outRows = 0;
    private int openedTxs = 0;
    private int closedTxs = 0;

    HandlerBatcher(Transaction rootTx) {
        this.rootTx = Objects.requireNonNull(rootTx);
        this.txImpl = new TransactionImpl();
    }

    public void commitAll() {
        rootTx.commit(inRows);
    }

    @Override
    public Transaction tx() {
        if (openedTxs != closedTxs) {
            throw new IllegalStateException();
        }
        openedTxs++;
        return new TransactionSafety(txImpl);
    }

    /**
     * This implementation by itself is not correct, as it doesn't enforce error-after-close. Needs to be combined with
     * something like {@link TransactionSafety}.
     */
    private class TransactionImpl implements Transaction {

        @Override
        public Chunks take(int minSize) {
            return rootTx.take(minSize);
        }

        @Override
        public void complete(Chunks chunks, int outRows) {
            rootTx.complete(chunks, outRows);
            HandlerBatcher.this.outRows += outRows;
        }

        @Override
        public void commit(int inRows) {
            HandlerBatcher.this.inRows += inRows;
        }

        @Override
        public void close() {
            ++HandlerBatcher.this.closedTxs;
        }
    }
}
