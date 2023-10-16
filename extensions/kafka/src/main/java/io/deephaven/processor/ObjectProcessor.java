/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.Type;

import java.time.Instant;
import java.util.List;

/**
 * An interface for processing data from one or more input objects into output chunks on a 1-to-1 input to output basis.
 *
 * @param <T> the object type
 * @see ObjectProcessorRowLimited
 */
public interface ObjectProcessor<T> {

    /**
     * Creates or returns an implementation that adds strict safety checks around {@code delegate}
     * {@link #processAll(ObjectChunk, List)}. The may be useful during for development or debugging purposes.
     *
     * @param delegate the delegate
     * @return the strict implementation
     * @param <T> the object type
     */
    static <T> ObjectProcessor<T> strict(ObjectProcessor<T> delegate) {
        return ObjectProcessorStrict.create(delegate);
    }

    /**
     * The relationship between {@link #outputTypes() output types} and the {@link #processAll(ObjectChunk, List)
     * processAll out param} {@link WritableChunk#getChunkType()}.
     *
     * <table>
     * <tr>
     * <th>{@link Type}</th>
     * <th>{@link ChunkType}</th>
     * </tr>
     * <tr>
     * <td>{@link ByteType}</td>
     * <td>{@link ChunkType#Byte}</td>
     * </tr>
     * <tr>
     * <td>{@link ShortType}</td>
     * <td>{@link ChunkType#Short}</td>
     * </tr>
     * <tr>
     * <td>{@link IntType}</td>
     * <td>{@link ChunkType#Int}</td>
     * </tr>
     * <tr>
     * <td>{@link LongType}</td>
     * <td>{@link ChunkType#Long}</td>
     * </tr>
     * <tr>
     * <td>{@link FloatType}</td>
     * <td>{@link ChunkType#Float}</td>
     * </tr>
     * <tr>
     * <td>{@link DoubleType}</td>
     * <td>{@link ChunkType#Double}</td>
     * </tr>
     * <tr>
     * <td>{@link CharType}</td>
     * <td>{@link ChunkType#Char}</td>
     * </tr>
     * <tr>
     * <td>{@link BooleanType}</td>
     * <td>{@link ChunkType#Byte} (<b>not</b> {@link ChunkType#Boolean})</td>
     * </tr>
     * <tr>
     * <td>{@link BoxedType}</td>
     * <td>Same as {@link BoxedType#primitiveType()} would yield.</td>
     * </tr>
     * <tr>
     * <td>{@link InstantType}</td>
     * <td>{@link ChunkType#Long} ({@link io.deephaven.time.DateTimeUtils#epochNanos(Instant)})</td>
     * </tr>
     * <tr>
     * <td>All other {@link GenericType}</td>
     * <td>{@link ChunkType#Object}</td>
     * </tr>
     * </table>
     */
    static ChunkType chunkType(Type<?> type) {
        return ObjectProcessorTypes.of(type);
    }

    /**
     * The logical output types {@code this} instance processes. The size and types correspond to the expected size and
     * {@link io.deephaven.chunk.ChunkType chunk types} for {@link #processAll(ObjectChunk, List)} as specified by
     * {@link #chunkType(Type)}.
     *
     * @return the output types
     */
    List<Type<?>> outputTypes();

    /**
     * Processes {@code in} into {@code out} by appending {@code in.size()} values to each chunk. The size of each
     * {@code out} chunk will be incremented by {@code in.size()}. Implementations are free to process the data in a
     * row-oriented, column-oriented, or mix-oriented fashion. Implementations must not keep any references to the
     * passed-in chunks.
     *
     * <p>
     * If an exception thrown the output chunks will be in an unspecified state.
     *
     * @param in the input objects
     * @param out the output chunks as specified by {@link #outputTypes()}; each chunk must have remaining capacity of
     *        at least {@code in.size()}
     */
    void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out);
}
