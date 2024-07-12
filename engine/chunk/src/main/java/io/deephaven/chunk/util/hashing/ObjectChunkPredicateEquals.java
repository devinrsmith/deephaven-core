//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;

import java.util.Objects;
import java.util.function.BiPredicate;

public class ObjectChunkPredicateEquals<T> implements ChunkEquals {

    private final BiPredicate<T, T> predicate;
    private final BiPredicate<T, T> notPredicate;

    public ObjectChunkPredicateEquals(BiPredicate<T, T> predicate) {
        this.predicate = Objects.requireNonNull(predicate);
        this.notPredicate = predicate.negate();
    }

    @Override
    public boolean equalReduce(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs) {
        return equalReduce(predicate, lhs.asObjectChunk(), rhs.asObjectChunk());
    }

    @Override
    public void equal(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        equal(predicate, lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    @Override
    public void equalNext(Chunk<? extends Any> chunk, WritableBooleanChunk destination) {
        equalNext(predicate, chunk.asObjectChunk(), destination);
    }

    @Override
    public void andEqual(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        andEqual(predicate, lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    @Override
    public void andEqualNext(Chunk<? extends Any> chunk, WritableBooleanChunk destination) {
        andEqualNext(predicate, chunk.asObjectChunk(), destination);
    }

    @Override
    public void equalPermuted(IntChunk<ChunkPositions> lhsPositions, IntChunk<ChunkPositions> rhsPositions,
            Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        equalPermuted(predicate, lhsPositions, rhsPositions, lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    @Override
    public void equalLhsPermuted(IntChunk<ChunkPositions> lhsPositions, Chunk<? extends Any> lhs,
            Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        equalLhsPermuted(predicate, lhsPositions, lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    @Override
    public void andEqualPermuted(IntChunk<ChunkPositions> lhsPositions, IntChunk<ChunkPositions> rhsPositions,
            Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        andEqualPermuted(predicate, lhsPositions, rhsPositions, lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    @Override
    public void andEqualLhsPermuted(IntChunk<ChunkPositions> lhsPositions, Chunk<? extends Any> lhs,
            Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        andEqualLhsPermuted(predicate, lhsPositions, lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    @Override
    public void notEqual(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        equal(notPredicate, lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    @Override
    public void andNotEqual(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        andEqual(notPredicate, lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    @Override
    public void equalPairs(IntChunk<ChunkPositions> chunkPositionsToCheckForEquality, Chunk<? extends Any> valuesChunk,
            WritableBooleanChunk destinations) {
        equalPairs(predicate, chunkPositionsToCheckForEquality, valuesChunk.asObjectChunk(), destinations);
    }

    @Override
    public void andEqualPairs(IntChunk<ChunkPositions> chunkPositionsToCheckForEquality,
            Chunk<? extends Any> valuesChunk, WritableBooleanChunk destinations) {
        andEqualPairs(predicate, chunkPositionsToCheckForEquality, valuesChunk.asObjectChunk(), destinations);
    }

    private static <T> boolean equalReduce(
            BiPredicate<T, T> predicate,
            ObjectChunk<T, ? extends Any> lhs,
            ObjectChunk<T, ? extends Any> rhs) {
        if (lhs.size() != rhs.size()) {
            return false;
        }
        for (int ii = 0; ii < lhs.size(); ++ii) {
            if (!predicate.test(lhs.get(ii), rhs.get(ii))) {
                return false;
            }
        }
        return true;
    }

    private static <T> int firstDifference(
            BiPredicate<T, T> predicate,
            ObjectChunk<T, ? extends Any> lhs,
            ObjectChunk<T, ? extends Any> rhs) {
        int ii;
        for (ii = 0; ii < lhs.size() && ii < rhs.size(); ++ii) {
            if (!predicate.test(lhs.get(ii), rhs.get(ii))) {
                return ii;
            }
        }
        return ii;
    }

    private static <T> void equal(
            BiPredicate<T, T> predicate,
            ObjectChunk<T, ? extends Any> lhs,
            ObjectChunk<T, ? extends Any> rhs,
            WritableBooleanChunk destination) {
        for (int ii = 0; ii < lhs.size(); ++ii) {
            destination.set(ii, predicate.test(lhs.get(ii), rhs.get(ii)));
        }
        destination.setSize(lhs.size());
    }

    private static <T> void equalNext(
            BiPredicate<T, T> predicate,
            ObjectChunk<T, ? extends Any> chunk,
            WritableBooleanChunk destination) {
        for (int ii = 0; ii < chunk.size() - 1; ++ii) {
            destination.set(ii, predicate.test(chunk.get(ii), chunk.get(ii + 1)));
        }
        destination.setSize(chunk.size() - 1);
    }

    private static <T> void equal(
            BiPredicate<T, T> predicate,
            ObjectChunk<T, ? extends Any> lhs, T rhs,
            WritableBooleanChunk destination) {
        for (int ii = 0; ii < lhs.size(); ++ii) {
            destination.set(ii, predicate.test(lhs.get(ii), rhs));
        }
        destination.setSize(lhs.size());
    }

    private static <T> void andEqual(
            BiPredicate<T, T> predicate,
            ObjectChunk<T, ? extends Any> lhs,
            ObjectChunk<T, ? extends Any> rhs,
            WritableBooleanChunk destination) {
        for (int ii = 0; ii < lhs.size(); ++ii) {
            destination.set(ii, destination.get(ii) && predicate.test(lhs.get(ii), rhs.get(ii)));
        }
        destination.setSize(lhs.size());
    }

    private static <T> void andEqualNext(
            BiPredicate<T, T> predicate,
            ObjectChunk<T, ? extends Any> chunk,
            WritableBooleanChunk destination) {
        for (int ii = 0; ii < chunk.size() - 1; ++ii) {
            destination.set(ii, destination.get(ii) && predicate.test(chunk.get(ii), chunk.get(ii + 1)));
        }
        destination.setSize(chunk.size() - 1);
    }

    private static <T> void equalPairs(
            BiPredicate<T, T> predicate,
            IntChunk<ChunkPositions> chunkPositionsToCheckForEquality,
            ObjectChunk<T, ? extends Any> valuesChunk,
            WritableBooleanChunk destinations) {
        final int pairCount = chunkPositionsToCheckForEquality.size() / 2;
        for (int ii = 0; ii < pairCount; ++ii) {
            final int firstPosition = chunkPositionsToCheckForEquality.get(ii * 2);
            final int secondPosition = chunkPositionsToCheckForEquality.get(ii * 2 + 1);
            final boolean equals = predicate.test(valuesChunk.get(firstPosition), valuesChunk.get(secondPosition));
            destinations.set(ii, equals);
        }
        destinations.setSize(pairCount);
    }

    private static <T> void andEqualPairs(
            BiPredicate<T, T> predicate,
            IntChunk<ChunkPositions> chunkPositionsToCheckForEquality,
            ObjectChunk<T, ? extends Any> valuesChunk,
            WritableBooleanChunk destinations) {
        final int pairCount = chunkPositionsToCheckForEquality.size() / 2;
        for (int ii = 0; ii < pairCount; ++ii) {
            if (destinations.get(ii)) {
                final int firstPosition = chunkPositionsToCheckForEquality.get(ii * 2);
                final int secondPosition = chunkPositionsToCheckForEquality.get(ii * 2 + 1);
                final boolean equals = predicate.test(valuesChunk.get(firstPosition), valuesChunk.get(secondPosition));
                destinations.set(ii, equals);
            }
        }
    }

    private static <T> void equalPermuted(
            BiPredicate<T, T> predicate,
            IntChunk<ChunkPositions> lhsPositions,
            IntChunk<ChunkPositions> rhsPositions,
            ObjectChunk<T, ? extends Any> lhs,
            ObjectChunk<T, ? extends Any> rhs,
            WritableBooleanChunk destinations) {
        for (int ii = 0; ii < lhsPositions.size(); ++ii) {
            final int lhsPosition = lhsPositions.get(ii);
            final int rhsPosition = rhsPositions.get(ii);
            final boolean equals = predicate.test(lhs.get(lhsPosition), rhs.get(rhsPosition));
            destinations.set(ii, equals);
        }
        destinations.setSize(lhsPositions.size());
    }

    private static <T> void andEqualPermuted(
            BiPredicate<T, T> predicate,
            IntChunk<ChunkPositions> lhsPositions,
            IntChunk<ChunkPositions> rhsPositions,
            ObjectChunk<T, ? extends Any> lhs,
            ObjectChunk<T, ? extends Any> rhs,
            WritableBooleanChunk destinations) {
        for (int ii = 0; ii < lhsPositions.size(); ++ii) {
            if (destinations.get(ii)) {
                final int lhsPosition = lhsPositions.get(ii);
                final int rhsPosition = rhsPositions.get(ii);
                final boolean equals = predicate.test(lhs.get(lhsPosition), rhs.get(rhsPosition));
                destinations.set(ii, equals);
            }
        }
        destinations.setSize(lhsPositions.size());
    }

    private static <T> void equalLhsPermuted(
            BiPredicate<T, T> predicate,
            IntChunk<ChunkPositions> lhsPositions,
            ObjectChunk<T, ? extends Any> lhs,
            ObjectChunk<T, ? extends Any> rhs,
            WritableBooleanChunk destinations) {
        for (int ii = 0; ii < lhsPositions.size(); ++ii) {
            final int lhsPosition = lhsPositions.get(ii);
            final boolean equals = predicate.test(lhs.get(lhsPosition), rhs.get(ii));
            destinations.set(ii, equals);
        }
        destinations.setSize(lhsPositions.size());
    }

    private static <T> void andEqualLhsPermuted(
            BiPredicate<T, T> predicate,
            IntChunk<ChunkPositions> lhsPositions,
            ObjectChunk<T, ? extends Any> lhs,
            ObjectChunk<T, ? extends Any> rhs, WritableBooleanChunk destinations) {
        for (int ii = 0; ii < lhsPositions.size(); ++ii) {
            if (destinations.get(ii)) {
                final int lhsPosition = lhsPositions.get(ii);
                final boolean equals = predicate.test(lhs.get(lhsPosition), rhs.get(ii));
                destinations.set(ii, equals);
            }
        }
        destinations.setSize(lhsPositions.size());
    }
}
