package io.deephaven.parquet.table;

import io.deephaven.base.verify.Assert;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.Type;

import java.util.Collection;
import java.util.List;
import java.util.Queue;

final class ParquetPath {

    interface Visitor {
        void accept(Collection<Type> path, ColumnDescriptor descriptor);
    }

    static void walk(Type type, Visitor visitor) {

        if (type.isPrimitive()) {
            visitor.accept(List.of(type), new ColumnDescriptor(new String[] { type.getName() }, type.asPrimitiveType(), 0, 0));
            return;
        }

    }

    private static void walk(Type type, Visitor visitor, Queue<Type> path, int maxRep, int maxDef) {
        if (type.isPrimitive()) {
            final String[] namePath = path.stream().map(Type::getName).toArray(String[]::new);
            visitor.accept(path, new ColumnDescriptor(namePath, type.asPrimitiveType(), 0, 0));
            return;
        }
        for (final Type field : type.asGroupType().getFields()) {
            Assert.eqTrue(path.offer(field), "path.offer(field)");
            walk(field, visitor, path, 0, 0);
            Assert.eq(path.poll(), "path.poll()", field, "field");
        }

    }



}
