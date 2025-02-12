//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public final class SchemaHelper {

    public static List<NestedField> fieldPath(Schema schema, List<Integer> idPath) {
        final List<NestedField> out = new ArrayList<>(idPath.size());
        if (idPath.isEmpty()) {
            return out;
        }
        NestedField current = schema.findField(idPath.get(0));
        if (current == null) {
            throw idPathNotFound(idPath, out);
        }
        out.add(current);
        for (final int id : idPath.subList(1, idPath.size())) {
            if (!current.type().isNestedType()) {
                throw idPathTooLong(idPath, out);
            }
            current = current.type().asNestedType().field(id);
            if (current == null) {
                throw idPathNotFound(idPath, out);
            }
            out.add(current);
        }
        return out;
    }

    private static IllegalArgumentException idPathNotFound(List<Integer> path, List<NestedField> context) {

        throw new IllegalArgumentException(
                String.format("id path not found, path=%s, context=%s", path.toString(), toNameString(context)));
    }

    static String toNameString(Collection<? extends NestedField> context) {
        return context.stream().map(NestedField::name).collect(Collectors.joining("', '", "['", "']"));
    }

    private static IllegalArgumentException idPathTooLong(List<Integer> path, List<NestedField> context) {
        throw new IllegalArgumentException(
                String.format("id path too long, path=%s, context=%s", path.toString(), toNameString(context)));
    }
}
