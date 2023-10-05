/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.chunk;

import io.deephaven.qst.type.Type;

import java.io.Closeable;
import java.util.List;


public interface MultiChunks<T> {

    /**
     * Creates a no-op implementation that consumes all of the input objects without producing any outputs.
     *
     * @param outputTypes the output types
     * @return the no-op implementation
     * @param <T> the object type
     */
    static <T> MultiChunks<T> noop(List<Type<?>> outputTypes) {
        return new MultiChunksNoOp<>(outputTypes);
    }

    List<Type<?>> outputTypes();

    /**
     *
     * @param in the input objects
     * @param handler the handler
     */
    void handleAll(ObjectChunk<? extends T, ?> in, Handler handler);

    interface Handler {

        /**
         * Create a transaction. Only one transaction
         *
         * @return the transaction
         */
        Transaction tx();
    }

    /**
     * A transaction represents the interface between the producer and consumer with respect to chunks. Chunks handed
     * off in a transaction guarantees that the downstream consumer will receive all of those chunks in-order and at the
     * same time. A transaction does not guarantee that only those chunks will be received by the consumer; ie,
     * transactions may be combined.
     */
    interface Transaction extends Closeable {

        // todo: implementations _may_ use this in a "threaded" manner as long as they linearize take/complete/commit/close
        // or, they may

        /**
         * Creates chunks with at least {@code minSize} remaining space. Callers are encouraged to be precise when they
         * know the number of output rows is small or has a small bound, and otherwise encouraged to take and complete
         * chunks in a buffered manner using a {@code minSize} of {@code min(bufferSize, precise)}.
         *
         * @param minSize the minimum size
         * @return the chunks, guaranteed to have {@link Chunks#size()} of at least {@code minSize}
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
         *
         * @param inRows the number of completed in rows that the comp
         */
        void commit(int inRows);

        /**
         * Close this transaction.
         */
        void close();
    }

    interface Chunks {
        /**
         *
         * @return
         */
        int size();
        List<WritableChunk<?>> out();
    }
}
