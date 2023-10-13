package io.deephaven.chunk;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class ChunksProviderSimple implements ChunksProvider {
    private final List<ChunkType> chunkTypes;
    private final Consumer<List<? extends WritableChunks>> onCommit;
    private final int desiredChunkSize;

    ChunksProviderSimple(
            List<ChunkType> chunkTypes,
            Consumer<List<? extends WritableChunks>> onCommit,
            int desiredChunkSize) {
        if (desiredChunkSize <= 0) {
            throw new IllegalArgumentException("desiredChunkSize must be positive");
        }
        this.chunkTypes = List.copyOf(Objects.requireNonNull(chunkTypes));
        this.onCommit = Objects.requireNonNull(onCommit);
        this.desiredChunkSize = desiredChunkSize;
    }

    @Override
    public List<ChunkType> chunkTypes() {
        return chunkTypes;
    }

    @Override
    public Transaction tx() {
        return new TransactionImpl();
    }

    protected class TransactionImpl extends TransactionBase<ChunksImpl> {
        private final List<ChunksImpl> full;
        private ChunksImpl recent;

        public TransactionImpl() {
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
            return recent = makeChunks(minSize);
        }

        @Override
        protected void completeImpl(ChunksImpl chunk, int outRows) {
            // todo: should we think about Chunks being mutable like ByteBuffer?
            if (outRows > 0) {
                recent = new ChunksImpl(chunk.out(), chunk.pos() + outRows, chunk.size());
            }
        }

        @Override
        protected void submitImpl() {
            if (recent != null) {
                full.add(recent);
                recent = null;
            }
            final List<WritableChunks> flipped = full.stream()
                    .map(ChunksImpl::flip)
                    .collect(Collectors.toList());
            try {
                onCommit.accept(flipped);
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
                WritableChunks.closeAll(Stream.concat(
                        full.stream(),
                        recent == null ? Stream.empty() : Stream.of(recent)));
            } finally {
                recent = null;
                full.clear();
            }
        }
    }

    private ChunksImpl makeChunks(int minSize) {
        return makeChunks(chunkTypes, Math.max(minSize, desiredChunkSize));
    }

    static ChunksImpl makeChunks(List<ChunkType> types, int size) {
        final List<WritableChunk<?>> chunks = types.stream()
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

    protected static class ChunksImpl implements WritableChunks {
        private final int pos;
        private final int size;
        private final List<WritableChunk<?>> out;

        ChunksImpl(List<WritableChunk<?>> out, int pos, int size) {
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

        WritableChunks flip() {
            return new ChunksImpl(out, 0, pos);
        }
    }
}
