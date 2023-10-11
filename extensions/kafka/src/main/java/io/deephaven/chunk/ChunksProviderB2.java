package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProviderSimple.ChunksImpl;
import io.deephaven.util.SafeCloseable;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ChunksProviderB2 implements ChunksProvider, Closeable {

    private final List<ChunkType> chunkTypes;
    private final Consumer<List<? extends WritableChunks>> onCommit;
    private final int desiredChunkSize;

    private final Lock lock;
    private final List<ChunksImpl> buffer;
    private volatile boolean closed;

    public ChunksProviderB2(
            List<ChunkType> chunkTypes,
            Consumer<List<? extends WritableChunks>> onCommit,
            int desiredChunkSize,
            boolean fairLock) {
        this.chunkTypes = List.copyOf(Objects.requireNonNull(chunkTypes));
        this.onCommit = Objects.requireNonNull(onCommit);
        this.desiredChunkSize = desiredChunkSize;
        // Re-entrancy not an important consideration, just easy to use this lock.
        // Implementation doesn't expose lock to outside, nor use in re-entrant manner.
        this.lock = new ReentrantLock(fairLock);
        this.buffer = new ArrayList<>();
        this.closed = false;
    }

    @Override
    public List<ChunkType> chunkTypes() {
        return chunkTypes;
    }

    @Override
    public Transaction tx() {
        throwIfClosed();
        lock.lock();
        try {
            return txImpl();
        } catch (Throwable t) {
            lock.unlock();
            throw t;
        }
    }

    @Override
    public void close() {
        // todo: should we provide other ways of closing? closeInterruptibly? tryClose? etc.
        if (closed) {
            return;
        }
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closeImpl();
        } finally {
            lock.unlock();
        }
    }

    public void commit() {
        throwIfClosed();
        lock.lock();
        try {
            commitImpl();
        } finally {
            lock.unlock();
        }
    }

    public void commitInterruptibly() throws InterruptedException {
        throwIfClosed();
        lock.lockInterruptibly();
        try {
            commitImpl();
        } finally {
            lock.unlock();
        }
    }

    public boolean tryCommit() {
        throwIfClosed();
        if (!lock.tryLock()) {
            return false;
        }
        try {
            commitImpl();
        } finally {
            lock.unlock();
        }
        return true;
    }

    public boolean tryCommit(Duration duration) throws InterruptedException {
        throwIfClosed();
        if (!lock.tryLock(duration.toNanos(), TimeUnit.NANOSECONDS)) {
            return false;
        }
        try {
            commitImpl();
        } finally {
            lock.unlock();
        }
        return true;
    }

    private BufferedTransaction txImpl() {
        // Precondition, must have lock
        throwIfClosed();
        return new BufferedTransaction();
    }

    private void commitImpl() {
        // Precondition, must have lock
        throwIfClosed();
        final List<WritableChunks> flipped = buffer.stream()
                .map(ChunksImpl::flip)
                .collect(Collectors.toList());
        try {
            onCommit.accept(flipped);
        } finally {
            // Even if accept fails, we assume that ownership has passed to the receiver.
            buffer.clear();
        }
    }

    private void closeImpl() {
        // Precondition, must have lock
        try {
            SafeCloseable.closeAll(buffer);
        } finally {
            buffer.clear();
            closed = true;
        }
    }

    private void throwIfClosed() {
        if (closed) {
            throw new IllegalStateException("closed");
        }
    }

    class BufferedTransaction extends TransactionBase<ChunksImpl> {

        private final List<ChunksImpl> previousBuffer;

        public BufferedTransaction() {
            // note: this copy only works right now b/c ChunksImpl pos/size is immutable
            previousBuffer = new ArrayList<>(buffer);
        }

        @Override
        protected ChunksImpl takeImpl(int minSize) {
            if (buffer.isEmpty()) {
                return newChunks(minSize);
            }
            final ChunksImpl existing = buffer.get(buffer.size() - 1);
            if (existing.remaining() >= minSize) {
                return existing;
            }
            return newChunks(minSize);
        }

        @Override
        protected void completeImpl(ChunksImpl chunk, int outRows) {
            if (outRows > 0) {
                buffer.set(buffer.size() - 1, new ChunksImpl(chunk.out(), chunk.pos() + outRows, chunk.size()));
            }
        }

        @Override
        protected void commitImpl() {
            // No-op.
            // We could consider some auto-commit threshold based on size of outRows / number of chunks, but then it
            // puts the burden of handling commit impl exceptions to us and what that means in the context of a
            // "rollback". In this manner, we are leaving any commit impl exception handler to the orchestration layer
            // (ie, the layer calling ChunksProviderB2.commit).
        }

        @Override
        protected void closeImpl(boolean committed, ChunksImpl outstanding, Throwable takeImplThrowable, Throwable completeImplThrowable, Throwable commitImplThrowable) {
            try {
                // note: if outstanding exists, it will be part of the rollback / closeAll if necessary
                final boolean successfulCommit =committed
                        && outstanding == null
                        && takeImplThrowable == null
                        && completeImplThrowable == null
                        && commitImplThrowable == null;
                if (!successfulCommit) {
                    rollback();
                }
            } finally {
                lock.unlock();
            }
        }

        private void rollback() {
            // Reset the state to what it was at beginning of transaction
            final List<ChunksImpl> newChunks = buffer.subList(previousBuffer.size(), buffer.size());
            try {
                SafeCloseable.closeAll(newChunks);
            } finally {
                buffer.clear();
                buffer.addAll(previousBuffer);
                previousBuffer.clear();
            }
        }
    }

    private ChunksImpl newChunks(int minSize) {
        return ChunksProviderSimple.makeChunks(chunkTypes, Math.max(minSize, desiredChunkSize));
    }
}
