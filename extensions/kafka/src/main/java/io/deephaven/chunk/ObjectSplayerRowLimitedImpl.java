package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.qst.type.Type;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

final class ObjectSplayerRowLimitedImpl<T> implements ObjectSplayerRowLimited<T> {
    private final ObjectSplayer<T> delegate;
    private final int chunkSize;

    ObjectSplayerRowLimitedImpl(ObjectSplayer<T> delegate, int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be positive");
        }
        if (chunkSize == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("chunkSize must be less than Integer.MAX_VALUE");
        }
        this.delegate = Objects.requireNonNull(delegate);
        this.chunkSize = chunkSize;
    }

    ObjectSplayer<T> delegate() {
        return delegate;
    }

    @Override
    public List<Type<?>> outputTypes() {
        return delegate.outputTypes();
    }

    @Override
    public int rowLimit() {
        return chunkSize;
    }

    @Override
    public void splayAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        for (ObjectChunk<? extends T, ?> slice : iterable(in, chunkSize)) {
            delegate.splayAll(slice, out);
        }
    }

    // todo: consider adding this functionality into Chunks themselves; potentially useful abstraction.

    static <T, ATTR extends Any> Iterable<ObjectChunk<T, ATTR>> iterable(ObjectChunk<T, ATTR> source,
            final int chunkSize) {
        // Note: we need to create the "for pool" version to guarantee we are getting a plain version; we know we don't
        // need to close them.
        return () -> iterator(source, ResettableObjectChunk.makeResettableChunkForPool(), chunkSize);
    }

    static <T, ATTR extends Any> Iterator<ObjectChunk<T, ATTR>> iterator(ObjectChunk<T, ATTR> source,
            ResettableObjectChunk<T, ATTR> slice, final int chunkSize) {
        return new ObjectChunkSliceIterator<>(source, slice, chunkSize);
    }

    private static class ObjectChunkSliceIterator<T, ATTR extends Any>
            implements CloseableIterator<ObjectChunk<T, ATTR>> {
        private final ObjectChunk<T, ATTR> source;
        private final ResettableObjectChunk<T, ATTR> slice;
        private final int chunkSize;
        private int ix = 0;

        private ObjectChunkSliceIterator(ObjectChunk<T, ATTR> source, ResettableObjectChunk<T, ATTR> slice,
                int chunkSize) {
            this.source = Objects.requireNonNull(source);
            this.slice = slice;
            this.chunkSize = chunkSize;
        }

        @Override
        public boolean hasNext() {
            return ix < source.size();
        }

        @Override
        public ObjectChunk<T, ATTR> next() {
            if (ix >= source.size() || ix < 0) {
                throw new NoSuchElementException();
            }
            slice.resetFromTypedChunk(source, ix, Math.min(chunkSize, source.size() - ix));
            ix += chunkSize;
            return slice;
        }
    }
}
