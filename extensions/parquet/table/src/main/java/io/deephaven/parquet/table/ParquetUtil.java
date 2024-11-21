package io.deephaven.parquet.table;

import io.deephaven.base.verify.Assert;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;

final class ParquetUtil {

    interface Visitor {
        void accept(Collection<Type> path, PrimitiveType primitiveType, int maxRep, int maxDef);
    }

    /**
     * A more efficient alternative to {@link MessageType#getColumns()}.
     */
    static void walk(MessageType type, Visitor visitor) {
        walk(type, visitor, new ArrayDeque<>(), 0, 0);
    }

    private static void walk(Type type, Visitor visitor, Queue<Type> path, int maxRep, int maxDef) {
        // This could be implemented more "efficiently" with switch / fallthrough or if else, but this is simpler to
        // understand.
        if (type.isRepetition(Type.Repetition.REPEATED)) {
            ++maxRep;
        }
        if (!type.isRepetition(Type.Repetition.REQUIRED)) {
            ++maxDef;
        }
        if (type.isPrimitive()) {
            visitor.accept(path, type.asPrimitiveType(), maxRep, maxDef);
            return;
        }
        walk(type.asGroupType(), visitor, path, maxRep, maxDef);
    }

    private static void walk(GroupType type, Visitor visitor, Queue<Type> path, int maxRep, int maxDef) {
        for (final Type field : type.getFields()) {
            Assert.eqTrue(path.offer(field), "path.offer(field)");
            walk(field, visitor, path, maxRep, maxDef);
            Assert.eq(path.poll(), "path.poll()", field, "field");
        }
    }
}
