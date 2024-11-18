package io.deephaven.parquet.format;

public interface Field {

    interface Visitor<T> {

        T visit(GroupField field);

        T visit(PrimitiveField field);
    }
}
