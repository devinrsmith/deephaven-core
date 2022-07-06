package io.deephaven.parquet.base;

import io.deephaven.parquet.base.util.Helpers;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;

import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

public final class BulkLongValuesWriterAdapter extends BulkValuesWriterDelegateBase<LongBuffer, Long> {

    public BulkLongValuesWriterAdapter(ValuesWriter writer) {
        super(writer);
    }

    @Override
    public void writeBulk(LongBuffer values, int rowCount) {
        for (int i = 0; i < rowCount; ++i) {
            writeLong(values.get());
        }
    }

    @Override
    public WriteResult writeBulkFilterNulls(LongBuffer bulkValues, Long nullValue, RunLengthBitPackingHybridEncoder dlEncoder, int rowCount) throws IOException {
        long nullLong = nullValue;
        while (bulkValues.hasRemaining()) {
            long next = bulkValues.get();
            if (next != nullLong) {
                writeLong(next);
                dlEncoder.writeInt(1);
            } else {
                dlEncoder.writeInt(0);
            }
        }
        return new WriteResult(rowCount);
    }

    @Override
    public WriteResult writeBulkFilterNulls(LongBuffer bulkValues, Long nullValue, int rowCount) {
        long nullLong = nullValue;
        IntBuffer nullOffsets = IntBuffer.allocate(4);
        int i = 0;
        while (bulkValues.hasRemaining()) {
            long next = bulkValues.get();
            if (next != nullLong) {
                writeLong(next);
            } else {
                nullOffsets = Helpers.ensureCapacity(nullOffsets);
                nullOffsets.put(i);
            }
            i++;
        }
        return new WriteResult(rowCount, nullOffsets);
    }
}
