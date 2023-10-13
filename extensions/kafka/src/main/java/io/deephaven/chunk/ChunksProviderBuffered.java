package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProviderSimple.ChunksImpl;
import io.deephaven.util.SafeCloseable;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class ChunksProviderBuffered implements ChunksProvider, Closeable {
    private static final Object CLOSED = new Object();

    private final List<ChunkType> chunkTypes;
    private final int desiredChunkSize;

    private final StampedLock sl;
    private final List<ChunksImpl> buffer;
    private volatile boolean closed;

    ChunksProviderBuffered(
            List<ChunkType> chunkTypes,
            int desiredChunkSize) {
        this.chunkTypes = List.copyOf(Objects.requireNonNull(chunkTypes));
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

    public List<WritableChunks> take() {
        throwIfClosed();
        return supplyLocked(this::takeImpl);
    }

    public List<WritableChunks> takeInterruptibly() throws InterruptedException {
        throwIfClosed();
        return supplyLockedInterruptibly(this::takeImpl);
    }

    public Optional<List<WritableChunks>> tryTake() {
        throwIfClosed();
        return Optional.ofNullable(trySupplyLocked(this::takeImpl));
    }

    public Optional<List<WritableChunks>> tryTake(Duration duration) throws InterruptedException {
        throwIfClosed();
        return Optional.ofNullable(trySupplyLocked(duration, this::takeImpl));
    }

    // --------------------------------------------------------------------------

    @Override
    public void close() {
        if (closed) {
            return;
        }
        supplyLocked(this::closeImpl);
    }

    public void closeInterruptibly() throws InterruptedException {
        if (closed) {
            return;
        }
        supplyLockedInterruptibly(this::closeImpl);
    }

    public boolean tryClose() {
        if (closed) {
            return true;
        }
        return trySupplyLocked(this::closeImpl) != null;
    }

    public boolean tryClose(Duration duration) throws InterruptedException {
        if (closed) {
            return true;
        }
        return trySupplyLocked(duration, this::closeImpl) != null;
    }

    // --------------------------------------------------------------------------

    private BufferedTransaction txImpl(long stamp) {
        // Precondition, must have write lock
        throwIfClosed();
        return new BufferedTransaction(stamp);
    }

    private List<WritableChunks> takeImpl() {
        // Precondition, must have write lock
        throwIfClosed();
        final List<WritableChunks> flipped = buffer.stream()
                .map(ChunksImpl::flip)
                .collect(Collectors.toList());
        buffer.clear();
        return flipped;
    }

    @SuppressWarnings("SameReturnValue")
    private Object closeImpl() {
        // Precondition, must have write lock
        if (closed) {
            return CLOSED;
        }
        try {
            WritableChunks.closeAll(buffer);
        } finally {
            buffer.clear();
            closed = true;
        }
        return CLOSED;
    }

    // --------------------------------------------------------------------------

    private void throwIfClosed() {
        if (closed) {
            throw new IllegalStateException("closed");
        }
    }

    private <T> T supplyLocked(Supplier<T> supplier) {
        final long stamp = sl.writeLock();
        try {
            return supplier.get();
        } finally {
            sl.unlockWrite(stamp);
        }
    }

    private <T> T supplyLockedInterruptibly(Supplier<T> supplier) throws InterruptedException {
        final long stamp = sl.writeLockInterruptibly();
        try {
            return supplier.get();
        } finally {
            sl.unlockWrite(stamp);
        }
    }

    private <T> T trySupplyLocked(Supplier<T> supplier) {
        final long stamp = sl.tryWriteLock();
        if (stamp == 0) {
            return null;
        }
        try {
            return supplier.get();
        } finally {
            sl.unlockWrite(stamp);
        }
    }

    private <T> T trySupplyLocked(Duration duration, Supplier<T> supplier) throws InterruptedException {
        final long stamp = sl.tryWriteLock(duration.toNanos(), TimeUnit.NANOSECONDS);
        if (stamp == 0) {
            return null;
        }
        try {
            return supplier.get();
        } finally {
            sl.unlockWrite(stamp);
        }
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
                WritableChunks.closeAll(newChunks);
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
