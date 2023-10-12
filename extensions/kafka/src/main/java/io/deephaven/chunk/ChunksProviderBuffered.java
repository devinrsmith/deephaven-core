package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProviderSimple.ChunksImpl;
import io.deephaven.util.SafeCloseable;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public final class ChunksProviderBuffered implements ChunksProvider, Closeable {

    private final List<ChunkType> chunkTypes;
    private final Consumer<List<? extends WritableChunks>> onFlush;
    private final int desiredChunkSize;

    private final StampedLock sl;
    private final List<ChunksImpl> buffer;
    private volatile boolean closed;

    ChunksProviderBuffered(
            List<ChunkType> chunkTypes,
            Consumer<List<? extends WritableChunks>> onFlush,
            int desiredChunkSize) {
        this.chunkTypes = List.copyOf(Objects.requireNonNull(chunkTypes));
        this.onFlush = Objects.requireNonNull(onFlush);
        this.desiredChunkSize = desiredChunkSize;
        this.sl = new StampedLock();
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
        final long stamp = sl.writeLock();
        try {
            return txImpl(stamp);
        } catch (Throwable t) {
            sl.unlockWrite(stamp);
            throw t;
        }
    }

    // --------------------------------------------------------------------------

    public void flush() {
        throwIfClosed();
        runLocked(this::flushImpl);
    }

    public void flushInterruptibly() throws InterruptedException {
        throwIfClosed();
        runLockedInterruptibly(this::flushImpl);
    }

    public boolean tryFlush() {
        throwIfClosed();
        return tryRunLocked(this::flushImpl);
    }

    public boolean tryFlush(Duration duration) throws InterruptedException {
        throwIfClosed();
        return tryRunLocked(duration, this::flushImpl);
    }

    // --------------------------------------------------------------------------

    @Override
    public void close() {
        if (closed) {
            return;
        }
        runLocked(this::closeImpl);
    }

    public void closeInterruptibly() throws InterruptedException {
        if (closed) {
            return;
        }
        runLockedInterruptibly(this::closeImpl);
    }

    public boolean tryClose() {
        if (closed) {
            return true;
        }
        return tryRunLocked(this::closeImpl);
    }

    public boolean tryClose(Duration duration) throws InterruptedException {
        if (closed) {
            return true;
        }
        return tryRunLocked(duration, this::closeImpl);
    }

    // --------------------------------------------------------------------------

    private BufferedTransaction txImpl(long stamp) {
        // Precondition, must have write lock
        throwIfClosed();
        return new BufferedTransaction(stamp);
    }

    private void flushImpl() {
        // Precondition, must have write lock
        throwIfClosed();
        final List<WritableChunks> flipped = buffer.stream()
                .map(ChunksImpl::flip)
                .collect(Collectors.toList());
        try {
            onFlush.accept(flipped);
        } finally {
            // Even if accept fails, we assume that ownership has passed to the receiver.
            buffer.clear();
        }
    }

    private void closeImpl() {
        // Precondition, must have write lock
        if (closed) {
            return;
        }
        try {
            SafeCloseable.closeAll(buffer);
        } finally {
            buffer.clear();
            closed = true;
        }
    }

    // --------------------------------------------------------------------------

    private void throwIfClosed() {
        if (closed) {
            throw new IllegalStateException("closed");
        }
    }

    private void runLocked(Runnable r) {
        final long stamp = sl.writeLock();
        try {
            r.run();
        } finally {
            sl.unlockWrite(stamp);
        }
    }

    private void runLockedInterruptibly(Runnable r) throws InterruptedException {
        final long stamp = sl.writeLockInterruptibly();
        try {
            r.run();
        } finally {
            sl.unlockWrite(stamp);
        }
    }

    private boolean tryRunLocked(Runnable r) {
        final long stamp = sl.tryWriteLock();
        if (stamp == 0) {
            return false;
        }
        try {
            r.run();
        } finally {
            sl.unlockWrite(stamp);
        }
        return true;
    }

    private boolean tryRunLocked(Duration duration, Runnable r) throws InterruptedException {
        final long stamp = sl.tryWriteLock(duration.toNanos(), TimeUnit.NANOSECONDS);
        if (stamp == 0) {
            return false;
        }
        try {
            r.run();
        } finally {
            sl.unlockWrite(stamp);
        }
        return true;
    }

    // --------------------------------------------------------------------------

    class BufferedTransaction extends TransactionBase<ChunksImpl> {

        private final long stamp;
        private final List<ChunksImpl> previousBuffer;

        BufferedTransaction(long stamp) {
            this.stamp = stamp;
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
        protected void submitImpl() {
            // No-op.
            // We could consider some auto-commit threshold based on size of outRows / number of chunks, but then it
            // puts the burden of handling commit impl exceptions to us and what that means in the context of a
            // "rollback". In this manner, we are leaving any commit impl exception handler to the orchestration layer
            // (ie, the layer calling ChunksProviderBuffered.flush).
        }

        @Override
        protected void closeImpl(boolean committed, ChunksImpl outstanding, Throwable takeImplThrowable,
                Throwable completeImplThrowable, Throwable commitImplThrowable) {
            try {
                // note: if outstanding exists, it will be part of the rollback / closeAll if necessary
                final boolean successfulCommit = committed
                        && outstanding == null
                        && takeImplThrowable == null
                        && completeImplThrowable == null
                        && commitImplThrowable == null;
                if (!successfulCommit) {
                    rollback();
                }
            } finally {
                sl.unlockWrite(stamp);
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
