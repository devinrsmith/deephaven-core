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
    private final int maxChunkSize;

    ObjectSplayerRowLimitedImpl(ObjectSplayer<T> delegate, int maxChunkSize) {
        if (maxChunkSize <= 0) {
            throw new IllegalArgumentException("maxChunkSize must be positive");
        }
        if (maxChunkSize == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("maxChunkSize must be less than Integer.MAX_VALUE");
        }
        this.delegate = Objects.requireNonNull(delegate);
        this.maxChunkSize = maxChunkSize;
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
        return maxChunkSize;
    }

    @Override
    public void splayAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        for (ObjectChunk<? extends T, ?> slice : iterable(in, maxChunkSize)) {
            delegate.splayAll(slice, out);
        }
    }

    // todo: consider adding this functionality into Chunks themselves; potentially useful abstraction.

    private static <T, ATTR extends Any> Iterable<ObjectChunk<T, ATTR>> iterable(ObjectChunk<T, ATTR> source,
            final int maxChunkSize) {
        // Note: we need to create the "for pool" version to guarantee we are getting a plain version; we know we don't
        // need to close them.
        return () -> iterator(source, ResettableObjectChunk.makeResettableChunkForPool(), maxChunkSize);
    }

    private static <T, ATTR extends Any> Iterator<ObjectChunk<T, ATTR>> iterator(ObjectChunk<T, ATTR> source,
            ResettableObjectChunk<T, ATTR> slice, final int maxChunkSize) {
        return new ObjectChunkSliceIterator<>(source, slice, maxChunkSize);
    }

    private static class ObjectChunkSliceIterator<T, ATTR extends Any>
            implements CloseableIterator<ObjectChunk<T, ATTR>> {
        private final ObjectChunk<T, ATTR> source;
        private final ResettableObjectChunk<T, ATTR> slice;
        private final int maxChunkSize;
        private int ix = 0;

        private ObjectChunkSliceIterator(ObjectChunk<T, ATTR> source, ResettableObjectChunk<T, ATTR> slice,
                int maxChunkSize) {
            this.source = Objects.requireNonNull(source);
            this.slice = slice;
            this.maxChunkSize = maxChunkSize;
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
            slice.resetFromTypedChunk(source, ix, Math.min(maxChunkSize, source.size() - ix));
            ix += maxChunkSize;
            return slice;
        }
    }
}
