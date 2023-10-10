package io.deephaven.chunk;

import java.io.Closeable;

public interface ChunksProvider {

    /**
     * Create a transaction. Only one transaction at a time.
     *
     * @return the transaction
     */
    Transaction tx(); // todo: parent tx?

    /**
     * A transaction represents the interface between the producer and consumer with respect to chunks. Chunks completed
     * as part of a committed transaction guarantees that the downstream consumer will receive all of those chunks
     * in-order and at the same time. A transaction does not guarantee that only those chunks will be received by the
     * consumer; ie, transactions may be combined.
     */
    interface Transaction extends Closeable {

        // todo: implementations _may_ use this in a "threaded" manner as long as they linearize take/complete/commit/close
        // or, they may

        // todo: callers are advised to only take/complete multiple if they need that larger scope for transaction purposes

        /**
         * Creates chunks with at least {@code minSize} remaining space. Callers are encouraged to be precise when they
         * know the number of output rows is small or has a small bound, and otherwise encouraged to take and complete
         * chunks in a buffered manner using a {@code minSize} of {@code min(bufferSize, precise)}.
         *
         * <p>
         * Implementations are encouraged to re-use chunks if there is enough space remaning.
         *
         * @param minSize the minimum size
         * @return the chunks, guaranteed to have {@link Chunks#remaining()} of at least {@code minSize}
         */
        Chunks take(int minSize);

        // todo: should this method be on Chunks instead?
        /**
         * Completes {@code chunks} as part of {@code this} transactions.
         *
         * @param chunks  the chunks
         * @param outRows the number of successful out rows appendend
         */
        void complete(Chunks chunks, int outRows);

        /**
         * Should be called once right before close.
         */
        void commit();

        /**
         * Close this transaction. Callers should ideally call this via a try-with-resources pattern.
         */
        void close();
    }
}
