package io.deephaven.parquet.base;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class BulkValuesWriterDelegateBase<T, L> extends AbstractBulkValuesWriter<T, L> {

    private final ValuesWriter writer;

    public BulkValuesWriterDelegateBase(ValuesWriter writer) {
        this.writer = writer;
    }

    @Override
    public ByteBuffer getByteBufferView() throws IOException {
        try {
            return getBytes().toByteBuffer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reset() {
        writer.reset();
    }

    @Override
    public long getBufferedSize() {
        return writer.getBufferedSize();
    }

    @Override
    public BytesInput getBytes() {
        return writer.getBytes();
    }

    @Override
    public Encoding getEncoding() {
        return writer.getEncoding();
    }

    @Override
    public long getAllocatedSize() {
        return writer.getAllocatedSize();
    }

    @Override
    public String memUsageString(String prefix) {
        return writer.memUsageString(prefix);
    }

    @Override
    public void close() {
        writer.close();
    }

    @Override
    public DictionaryPage toDictPageAndClose() {
        return writer.toDictPageAndClose();
    }

    @Override
    public void resetDictionary() {
        writer.resetDictionary();
    }

    @Override
    public void writeByte(int value) {
        writer.writeByte(value);
    }

    @Override
    public void writeBoolean(boolean v) {
        writer.writeBoolean(v);
    }

    @Override
    public void writeBytes(Binary v) {
        writer.writeBytes(v);
    }

    @Override
    public void writeInteger(int v) {
        writer.writeInteger(v);
    }

    @Override
    public void writeLong(long v) {
        writer.writeLong(v);
    }

    @Override
    public void writeDouble(double v) {
        writer.writeDouble(v);
    }

    @Override
    public void writeFloat(float v) {
        writer.writeFloat(v);
    }
}
