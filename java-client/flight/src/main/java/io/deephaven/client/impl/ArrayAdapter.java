package io.deephaven.client.impl;

import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.BooleanArray;
import io.deephaven.qst.array.ByteArray;
import io.deephaven.qst.array.CharArray;
import io.deephaven.qst.array.DoubleArray;
import io.deephaven.qst.array.FloatArray;
import io.deephaven.qst.array.GenericArray;
import io.deephaven.qst.array.IntArray;
import io.deephaven.qst.array.LongArray;
import io.deephaven.qst.array.PrimitiveArray;
import io.deephaven.qst.array.ShortArray;
import io.deephaven.qst.column.Column;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;

import java.util.Objects;

public class ArrayAdapter implements Array.Visitor, PrimitiveArray.Visitor {

    public static FieldVector of(String name, Array<?> array, RootAllocator allocator) {
        return array.walk(new ArrayAdapter(name, allocator)).out();
    }

    private final String name;
    private final RootAllocator allocator;

    private FieldVector out;

    ArrayAdapter(String name, RootAllocator allocator) {
        this.name = Objects.requireNonNull(name);
        this.allocator = Objects.requireNonNull(allocator);
    }

    FieldVector out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(PrimitiveArray<?> primitive) {
        primitive.walk((PrimitiveArray.Visitor) this);
    }

    @Override
    public void visit(GenericArray<?> generic) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(ByteArray byteArray) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(BooleanArray booleanArray) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(CharArray charArray) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(ShortArray shortArray) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(IntArray intArray) {
        out = VectorHelper.of(allocator, name, intArray.values(), 0, intArray.size());
    }

    @Override
    public void visit(LongArray longArray) {
        out = VectorHelper.of(allocator, name, longArray.values(), 0, longArray.size());
    }

    @Override
    public void visit(FloatArray floatArray) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(DoubleArray doubleArray) {
        throw new UnsupportedOperationException();
    }
}
