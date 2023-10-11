package io.deephaven.chunk;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Chunks provider is a pattern for producers or adapters to {@link Transaction#take(int) take},
 * {@link Transaction#complete(WritableChunks, int) complete}, and {@link Transaction#commit() commit}
 * {@link WritableChunks chunks} in an efficient manner.
 *
 * <p>
 * For example, in on-demand use cases where the caller knows they have a small fixed or small variable size to fill:
 *
 * <pre>
 * try (final Transaction tx = chunksProvider.tx()) {
 *     final WritableChunks chunks = tx.take(smallSize);
 *     final int outRows = doFill(chunks);
 *     tx.complete(chunks, outRows);
 *     tx.commit();
 * }
 * </pre>
 *
 * In pre-buffered use cases where the caller knows they have a large fixed or large variable size to fill:
 * <pre>
 * try (final Transaction tx = chunksProvider.tx()) {
 *     while (remaining() > 0) {
 *         final WritableChunks chunks = tx.take(Math.min(reasonableFixedSize, remaining()));
 *         final int outRows = doFill(chunks);
 *         tx.complete(chunks, outRows);
 *     }
 *     tx.commit();
 * }
 * </pre>
 *
 * TODO: should we even show this third case? it's a bit more esoteric, but _might_ be preferred?
 * <p>
 * In the case where the caller knows they have a large fixed or variable size and the full output doesn't need
 * to be transactional:
 *
 * <pre>
 * while (remaining() > 0) {
 *     try (final Transaction tx = chunksProvider.tx()) {
 *         final WritableChunks chunks = tx.take(Math.min(reasonableFixedSize, remaining()));
 *         final int outRows = doFill(chunks);
 *         tx.complete(chunks, outRows);
 *         tx.commit();
 *     }
 * }
 * </pre>
 */
public interface ChunksProvider {

    /**
     * Creates a chunks provider implementation that provides {@link Transaction#take(int) chunk take} reuse and simple
     * commit semantics - on {@link Transaction#commit() commit}, the chunks are passed off to {@code onCommitConsumer}.
     * As such, any threading guarantees depend on {@code onCommitConsumer}.
     *
     * <p>
     * This implementation is well-suited for use-cases where the caller already has some buffering logic, and can
     * commit reasonably sized transactions.
     *
     * @param chunkTypes the chunk types
     * @param onCommitConsumer the on-commit consumer
     * @param desiredChunkSize the desired chunk size
     * @return the simple chunks provider
     */
    static ChunksProvider ofSimple(
            List<ChunkType> chunkTypes, Consumer<List<? extends WritableChunks>> onCommitConsumer, int desiredChunkSize) {
        return new ChunksProviderSimple(chunkTypes, onCommitConsumer, desiredChunkSize);
    }

    /**
     * Creates a chunks provider implementation that provides {@link Transaction#take(int) chunk take} reuse and
     * buffered commit semantics - on {@link Transaction#commit() commit}, the chunks are registered, passed off to
     * {@code onCommitConsumer} on a delayed basis. ... todo
     *
     * <p>
     * This implementation is well-suited for use-cases where the caller does not have buffering logic, and commits
     * smaller sized transactions on-demand.
     */
    static ChunksProvider ofBuffered(
            List<ChunkType> chunkTypes,
            Consumer<List<? extends WritableChunks>> onCommitConsumer,
            int desiredChunkSize,
            Supplier<CountDownLatch> onFlush) {
        return null;
    }


    /**
     * The chunk types this provider creates.
     *
     * @return the chunk types
     */
    List<ChunkType> chunkTypes();


    // todo: probably want to change the name away from "transaction"?
    // the semantic guarantees of 'commit' are a bit untraditional - commit means that _if_ the downstream consumer gets
    // them, all chunks in a transaction are guaranteed to be delivired together. we _don't_ have the idea of a
    // transaction id or the idea that 'commit' atomically delivers the contents right now.

    /**
     * Create a transaction. Callers are strongly encouraged to use the try-with-resources pattern.
     *
     * <p>
     * Transactions are expected to be short-lived; they should ideally be opened, completed, and committed without
     * blocking, IO, or heavy computation.
     *
     * @return the transaction
     */
    Transaction tx();

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
         * Creates chunks with at least {@code minSize} remaining space to be populated by the caller. The chunks may be
         * populated in a row-oriented, column-oriented, or mix-oriented fashion. Guaranteed to have types as specified
         * by {@link #chunkTypes()}.
         *
         * <p>
         * Callers are encouraged to be precise when they know the number of output rows is small or has a small bound,
         * and otherwise encouraged to take and complete chunks in a buffered manner using a {@code minSize} of
         * {@code min(bufferSize, precise)}.
         *
         * <p>
         * Implementations are encouraged to re-use chunks when possible.
         *
         * @param minSize the minimum size
         * @return the chunks, guaranteed to have {@link WritableChunks#remaining()} of at least {@code minSize}
         */
        WritableChunks take(int minSize);

        // todo: should this method be on Chunks instead?
        /**
         * Completes {@code chunks} as part of {@code this} transactions.
         *
         * @param chunks  the chunks
         * @param outRows the number of successful out rows appendend
         */
        void complete(WritableChunks chunks, int outRows);

        // is "register" a better name?
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
