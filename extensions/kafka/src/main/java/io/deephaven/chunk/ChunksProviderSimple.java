package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProviderSimple.TransactionImpl;
import io.deephaven.util.SafeCloseable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class ChunksProviderSimple extends ChunksProviderBase<TransactionImpl> {

    private final List<ChunkType> chunkTypes;
    private final Consumer<List<? extends Chunks>> onCommit;
    private final int desiredChunkSize;

    public ChunksProviderSimple(List<ChunkType> chunkTypes, Consumer<List<? extends Chunks>> onCommit, int desiredChunkSize) {
        this.chunkTypes = Objects.requireNonNull(chunkTypes);
        this.onCommit = Objects.requireNonNull(onCommit);
        this.desiredChunkSize = desiredChunkSize;
    }

    @Override
    protected TransactionImpl txImpl(Consumer<TransactionImpl> onClosed) {
        return new TransactionImpl(onClosed);
    }

    protected class TransactionImpl extends TransactionBase<ChunksImpl> {
        private final Consumer<TransactionImpl> onClosed;
        private final List<ChunksImpl> full;
        private ChunksImpl recent;

        public TransactionImpl(Consumer<TransactionImpl> onClosed) {
            this.onClosed = Objects.requireNonNull(onClosed);
            this.full = new ArrayList<>();
        }

        @Override
        protected ChunksImpl takeImpl(int minSize) {
            if (recent != null) {
                if (recent.remaining() >= minSize) {
                    return recent;
                }
                full.add(recent);
            }
            return recent = makeChunks(Math.max(minSize, desiredChunkSize));
        }

        @Override
        protected void completeImpl(ChunksImpl chunk, int outRows) {
            // todo: should we think about Chunks being mutable like ByteBuffer?
            recent = new ChunksImpl(chunk.out(), chunk.pos() + outRows, chunk.size());
        }

        @Override
        protected void commitImpl() {
            if (recent != null) {
                full.add(recent);
                recent = null;
            }
            try {
                onCommit.accept(full);
            } finally {
                // Even if accept fails, we assume that ownership has passed to the receiver.
                full.clear();
            }
        }

        @Override
        protected void closeImpl(
                boolean committed,
                ChunksImpl outstanding,
                Throwable takeImplThrowable,
                Throwable completeImplThrowable,
                Throwable commitImplThrowable) {
            // outstanding, if present, will have the same contents as recent; no need to release it.
            try {
                SafeCloseable.closeAll(Stream.concat(
                        full.stream(),
                        recent == null ? Stream.empty() : Stream.of(recent)));
            } finally {
                recent = null;
                full.clear();
            }
            onClosed.accept(this);
        }
    }

    private ChunksImpl makeChunks(int size) {
        final List<WritableChunk<?>> chunks = chunkTypes.stream()
                .map(c -> c.makeWritableChunk(size))
                .collect(Collectors.toList());
        // todo: can we get a version of makeWritabhleChunk that returns the physical size instead of requested size?
        for (WritableChunk<?> chunk : chunks) {
            if (chunk.size() != size) {
                throw new IllegalStateException("Expected making chunks to return the exact same size...");
            }
            chunk.setSize(0);
        }
        return new ChunksImpl(chunks, 0, size);
    }

    protected static class ChunksImpl implements Chunks, SafeCloseable {
        private final int pos;
        private final int size;
        private final List<WritableChunk<?>> out;

        private ChunksImpl(List<WritableChunk<?>> out, int pos, int size) {
            this.out = List.copyOf(out);
            this.pos = pos;
            this.size = size;
        }

        @Override
        public int pos() {
            return pos;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public List<WritableChunk<?>> out() {
            return out;
        }

        @Override
        public void close() {
            SafeCloseable.closeAll(out);
        }
    }
}
