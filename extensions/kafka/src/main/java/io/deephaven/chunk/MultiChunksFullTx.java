package io.deephaven.chunk;

import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultiChunksFullTx<T> implements MultiChunks<T> {
    private final MultiChunks<T> impl;

    public MultiChunksFullTx(MultiChunks<T> impl) {
        this.impl = Objects.requireNonNull(impl);
    }

    @Override
    public List<Type<?>> outputTypes() {
        return impl.outputTypes();
    }

    @Override
    public void handleAll(ObjectChunk<? extends T, ?> in, Handler handler) {
        try (final Transaction rootTx = handler.tx()) {
            final SingleShotHandler singleShot = new SingleShotHandler(rootTx);
            impl.handleAll(in, singleShot);
            singleShot.commitAll();
        }
    }

    private static class SingleShotHandler implements Handler, Transaction {
        private final Transaction rootTx;
        private final AtomicBoolean outstandingTx = new AtomicBoolean(false);
        private final AtomicBoolean outstandingChunk = new AtomicBoolean(false);
        private int committedRows = 0;

        private SingleShotHandler(Transaction rootTx) {
            this.rootTx = Objects.requireNonNull(rootTx);
        }

        void commitAll() {
            rootTx.commit(committedRows);
        }

        // Handler impl

        @Override
        public Transaction tx() {
            if (!outstandingTx.compareAndSet(false, true)) {
                throw new IllegalStateException();
            }
            // todo: we could return a new one to help w/ some state management
            return this;
        }

        // Transaction impl

        @Override
        public Chunks take(int minSize) {
            if (!outstandingChunk.compareAndSet(false, true)) {
                throw new IllegalStateException();
            }
            return rootTx.take(minSize);
        }

        @Override
        public void complete(Chunks chunks, int outRows) {
            if (!outstandingChunk.compareAndSet(true, false)) {
                throw new IllegalStateException();
            }
            rootTx.complete(chunks, outRows);
        }

        @Override
        public void commit(int inRows) {
            if (outstandingChunk.get()) {
                throw new IllegalStateException("Must not commit with an outstanding chunk, please complete");
            }
            if (!outstandingTx.get()) {
                throw new IllegalStateException("Must not commit against old transaction");
            }
            committedRows += inRows;
        }

        @Override
        public void close() {
            if (!outstandingTx.compareAndSet(true, false)) {
                throw new IllegalStateException();
            }
        }
    }
}
