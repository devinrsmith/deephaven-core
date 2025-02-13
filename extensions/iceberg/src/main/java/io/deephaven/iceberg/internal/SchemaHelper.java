//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public final class SchemaHelper {

    public static List<NestedField> fieldPath(Schema schema, int[] idPath) {
        final List<NestedField> out = new ArrayList<>(idPath.length);
        if (idPath.length == 0) {
            return out;
        }
        NestedField current = schema.findField(idPath[0]);
        if (current == null) {
            throw idPathNotFound(idPath, out);
        }
        out.add(current);
        for (int i = 1; i < idPath.length; ++i) {
            if (!current.type().isNestedType()) {
                throw idPathTooLong(idPath, out);
            }
            current = current.type().asNestedType().field(idPath[i]);
            if (current == null) {
                throw idPathNotFound(idPath, out);
            }
            out.add(current);
        }
        return out;
    }

    static String toNameString(Collection<? extends NestedField> context) {
        return context.stream().map(NestedField::name).collect(Collectors.joining("', '", "['", "']"));
    }

    private static IllegalArgumentException idPathNotFound(int[] path, List<NestedField> context) {
        throw new IllegalArgumentException(
                String.format("id path not found, path=%s, context=%s", Arrays.toString(path), toNameString(context)));
    }

    private static IllegalArgumentException idPathTooLong(int[] path, List<NestedField> context) {
        throw new IllegalArgumentException(
                String.format("id path too long, path=%s, context=%s", Arrays.toString(path), toNameString(context)));
    }
}
