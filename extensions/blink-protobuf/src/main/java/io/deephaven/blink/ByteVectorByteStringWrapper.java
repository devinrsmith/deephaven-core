package io.deephaven.blink;

import com.google.protobuf.ByteString;
import io.deephaven.vector.ByteSubVector;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.ByteVectorDirect;
import io.deephaven.vector.ByteVectorSlice;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

class ByteVectorByteStringWrapper implements ByteVector {
    private final ByteString bs;

    public ByteVectorByteStringWrapper(ByteString bs) {
        this.bs = bs;
    }

    @Override
    public long size() {
        return bs.size();
    }

    @Override
    public byte get(long index) {
        if (index < 0 || index >= bs.size()) {
            return NULL_BYTE;
        }
        return bs.byteAt((int) index);
    }

    @Override
    public ByteVector subVector(long fromIndexInclusive, long toIndexExclusive) {
        return new ByteVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    @Override
    public ByteVector subVectorByPositions(long[] positions) {
        return new ByteSubVector(this, positions);
    }

    @Override
    public byte[] toArray() {
        return bs.toByteArray();
    }

    @Override
    public byte[] copyToArray() {
        return bs.toByteArray();
    }

    @Override
    public ByteVector getDirect() {
        return new ByteVectorDirect(toArray());
    }
}
