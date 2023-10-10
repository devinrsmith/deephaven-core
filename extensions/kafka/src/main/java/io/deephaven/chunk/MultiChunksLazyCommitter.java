package io.deephaven.chunk;

import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;

public class MultiChunksLazyCommitter<T> implements ObjectChunksOneToMany<T> {
    private final ObjectChunksOneToMany<T> impl;
    private final int minCommitSize;

    public MultiChunksLazyCommitter(ObjectChunksOneToMany<T> impl, int minCommitSize) {
        this.impl = Objects.requireNonNull(impl);
        this.minCommitSize = minCommitSize;
    }

    @Override
    public List<Type<?>> outputTypes() {
        return impl.outputTypes();
    }

//    @Override
//    public void handleAll(ObjectChunk<? extends T, ?> in, ChunksProvider handler) {
//        try (final ChunksProviderBatch batcher = new MyBatcher(handler)) {
//            impl.handleAll(in, batcher);
//            batcher.commit();
//        }
//    }

//    private class MyBatcher extends ChunksProviderBatch {
//
//        public MyBatcher(ChunksProvider handler) {
//            super(handler);
//        }
//
//        @Override
//        protected boolean shouldCommit(TransactionImpl transaction) {
//            return transaction.outstandingOutRows() >= minCommitSize;
//        }
//    }
}
