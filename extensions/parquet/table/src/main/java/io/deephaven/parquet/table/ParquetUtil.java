package io.deephaven.parquet.table;

import io.deephaven.base.verify.Assert;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Predicate;

final class ParquetUtil {

    interface Visitor {
        void accept(Collection<Type> path, PrimitiveType primitiveType);
    }


    static class ColumnDescriptorVisitor implements Visitor {

        private final Consumer<ColumnDescriptor> consumer;

        public ColumnDescriptorVisitor(Consumer<ColumnDescriptor> consumer) {
            this.consumer = Objects.requireNonNull(consumer);
        }

        @Override
        public void accept(Collection<Type> path, PrimitiveType primitiveType) {
            consumer.accept(makeColumnDescriptor(path, primitiveType));
        }
    }

    static ColumnDescriptor makeColumnDescriptor(Collection<Type> path, PrimitiveType primitiveType) {
        final String[] namePath = path.stream().map(Type::getName).toArray(String[]::new);
        final int maxRep = (int) path.stream().filter(ParquetUtil::isRepeated).count();
        final int maxDef = (int) path.stream().filter(Predicate.not(ParquetUtil::isRequired)).count();
        return new ColumnDescriptor(namePath, primitiveType, maxRep, maxDef);
    }

    /**
     * A more efficient implementation of {@link MessageType#getColumns()}.
     *
     * @param schema the message schema
     */
    static List<ColumnDescriptor> getColumns(MessageType schema) {
        final List<ColumnDescriptor> out = new ArrayList<>();
        walk(schema, new ColumnDescriptorVisitor(out::add));
        return out;
    }

    static void walk(MessageType type, Visitor visitor) {
        walk(type, visitor, new ArrayDeque<>());
    }

    private static void walk(Type type, Visitor visitor, Deque<Type> path) {
        if (type.isPrimitive()) {
            visitor.accept(path, type.asPrimitiveType());
            return;
        }
        walk(type.asGroupType(), visitor, path);
    }

    private static void walk(GroupType type, Visitor visitor, Deque<Type> path) {
        for (final Type field : type.getFields()) {
            Assert.eqTrue(path.offerLast(field), "path.offerLast(field)");
            walk(field, visitor, path);
            Assert.eq(path.pollLast(), "path.pollLast()", field, "field");
        }
    }

    private static boolean isRepeated(Type x) {
        return x.isRepetition(Repetition.REPEATED);
    }

    private static boolean isRequired(Type x) {
        return x.isRepetition(Repetition.REQUIRED);
    }
}
