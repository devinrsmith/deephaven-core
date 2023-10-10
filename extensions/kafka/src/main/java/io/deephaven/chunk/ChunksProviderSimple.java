package io.deephaven.chunk;

import io.deephaven.util.SafeCloseable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ChunksProviderSimple implements ChunksProvider {

    private final List<ChunkType> chunkTypes;
    private final Consumer<List<? extends Chunks>> onCommit;
    private final int desiredChunkSize;

    public ChunksProviderSimple(List<ChunkType> chunkTypes, Consumer<List<? extends Chunks>> onCommit) {
        this.chunkTypes = Objects.requireNonNull(chunkTypes);
        this.onCommit = Objects.requireNonNull(onCommit);
    }

    @Override
    public Transaction tx() {
        return new TransactionImpl();
    }

    private class TransactionImpl extends TransactionBase<ChunksImpl> {
        private final List<ChunksImpl> full = new ArrayList<>();
        private ChunksImpl recent;

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
            try {
                SafeCloseable.closeAll(Stream.concat(
                        full.stream(),
                        outstanding == null ? Stream.empty() : Stream.of(outstanding)));
            } finally {
                full.clear();
            }
        }
    }

    private ChunksImpl makeChunks(int minSize) {
        final List<WritableChunk<?>> chunks = chunkTypes.stream()
                .map(c -> c.makeWritableChunk(minSize))
                .collect(Collectors.toList());
        // todo: can we get a version of makeWritabhleChunk that returns the physical size instead of requested size?
        for (WritableChunk<?> chunk : chunks) {
            if (chunk.size() != minSize) {
                throw new IllegalStateException("Expected making chunks to return the exact same size...");
            }
            chunk.setSize(0);
        }
        return new ChunksImpl(chunks, 0, minSize);
    }

    private static class ChunksImpl implements Chunks, SafeCloseable {
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
