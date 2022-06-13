package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.tuple.generated.ObjectObjectByteTuple;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Object, Object, and Byte.
 * <p>Generated by io.deephaven.replicators.TupleSourceCodeGenerator.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ObjectObjectReinterpretedBooleanColumnTupleSource extends AbstractTupleSource<ObjectObjectByteTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ObjectObjectReinterpretedBooleanColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ObjectObjectByteTuple, Object, Object, Byte> FACTORY = new Factory();

    private final ColumnSource<Object> columnSource1;
    private final ColumnSource<Object> columnSource2;
    private final ColumnSource<Byte> columnSource3;

    public ObjectObjectReinterpretedBooleanColumnTupleSource(
            @NotNull final ColumnSource<Object> columnSource1,
            @NotNull final ColumnSource<Object> columnSource2,
            @NotNull final ColumnSource<Byte> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ObjectObjectByteTuple createTuple(final long rowKey) {
        return new ObjectObjectByteTuple(
                columnSource1.get(rowKey),
                columnSource2.get(rowKey),
                columnSource3.getByte(rowKey)
        );
    }

    @Override
    public final ObjectObjectByteTuple createPreviousTuple(final long rowKey) {
        return new ObjectObjectByteTuple(
                columnSource1.getPrev(rowKey),
                columnSource2.getPrev(rowKey),
                columnSource3.getPrevByte(rowKey)
        );
    }

    @Override
    public final ObjectObjectByteTuple createTupleFromValues(@NotNull final Object... values) {
        return new ObjectObjectByteTuple(
                values[0],
                values[1],
                BooleanUtils.booleanAsByte((Boolean)values[2])
        );
    }

    @Override
    public final ObjectObjectByteTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ObjectObjectByteTuple(
                values[0],
                values[1],
                TypeUtils.unbox((Byte)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ObjectObjectByteTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) BooleanUtils.byteAsBoolean(tuple.getThirdElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final ObjectObjectByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        if (elementIndex == 2) {
            return BooleanUtils.byteAsBoolean(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ObjectObjectByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<Values> [] chunks) {
        WritableObjectChunk<ObjectObjectByteTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<Object, Values> chunk1 = chunks[0].asObjectChunk();
        ObjectChunk<Object, Values> chunk2 = chunks[1].asObjectChunk();
        ByteChunk<Values> chunk3 = chunks[2].asByteChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ObjectObjectByteTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ObjectObjectReinterpretedBooleanColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ObjectObjectByteTuple, Object, Object, Byte> {

        private Factory() {
        }

        @Override
        public TupleSource<ObjectObjectByteTuple> create(
                @NotNull final ColumnSource<Object> columnSource1,
                @NotNull final ColumnSource<Object> columnSource2,
                @NotNull final ColumnSource<Byte> columnSource3
        ) {
            return new ObjectObjectReinterpretedBooleanColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
