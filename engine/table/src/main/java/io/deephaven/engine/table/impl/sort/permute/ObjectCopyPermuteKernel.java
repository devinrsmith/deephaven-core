//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort.permute;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;

import java.util.Objects;
import java.util.function.Function;

public final class ObjectCopyPermuteKernel<X> implements PermuteKernel {
    public static <X> PermuteKernel of(Function<X, X> copyFunction) {
        return new ObjectCopyPermuteKernel<>(copyFunction);
    }

    private final Function<X, X> f;

    private ObjectCopyPermuteKernel(Function<X, X> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public <T extends Any> void permute(
            Chunk<? extends T> inputValues,
            IntChunk<ChunkPositions> outputPositions,
            WritableChunk<? super T> outputValues) {
        ObjectCopyPermuteKernel.permute(f, inputValues.asObjectChunk(), outputPositions, outputValues.asWritableObjectChunk());
    }

    @Override
    public <T extends Any> void permute(
            IntChunk<ChunkPositions> inputPositions,
            Chunk<? extends T> inputValues,
            IntChunk<ChunkPositions> outputPositions,
            WritableChunk<? super T> outputValues) {
        ObjectCopyPermuteKernel.permute(f, inputPositions, inputValues.asObjectChunk(), outputPositions, outputValues.asWritableObjectChunk());
    }

    @Override
    public <T extends Any> void permuteInput(
            Chunk<? extends T> inputValues,
            IntChunk<ChunkPositions> inputPositions,
            WritableChunk<? super T> outputValues) {
        ObjectCopyPermuteKernel.permuteInput(f, inputValues.asObjectChunk(), inputPositions, outputValues.asWritableObjectChunk());
    }

    private static<TYPE_T, T extends Any> void permute(
            Function<TYPE_T, TYPE_T> copyFunction,
            ObjectChunk<TYPE_T, ? extends T> inputValues,
            IntChunk<ChunkPositions> outputPositions,
            WritableObjectChunk<TYPE_T, ? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), copyFunction.apply(inputValues.get(ii)));
        }
    }

    private static<TYPE_T, T extends Any> void permuteInput(
            Function<TYPE_T, TYPE_T> copyFunction,
            ObjectChunk<TYPE_T, ? extends T> inputValues,
            IntChunk<ChunkPositions> inputPositions,
            WritableObjectChunk<TYPE_T, ? super T> outputValues) {
        for (int ii = 0; ii < inputPositions.size(); ++ii) {
            outputValues.set(ii, copyFunction.apply(inputValues.get(inputPositions.get(ii))));
        }
    }

    private static<TYPE_T, T extends Any> void permute(
            Function<TYPE_T, TYPE_T> copyFunction,
            IntChunk<ChunkPositions> inputPositions,
            ObjectChunk<TYPE_T, ? extends T> inputValues,
            IntChunk<ChunkPositions> outputPositions,
            WritableObjectChunk<TYPE_T, ? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), copyFunction.apply(inputValues.get(inputPositions.get(ii))));
        }
    }
}
