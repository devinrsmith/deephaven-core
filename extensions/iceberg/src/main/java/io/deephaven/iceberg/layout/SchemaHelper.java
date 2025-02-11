package io.deephaven.iceberg.layout;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class SchemaHelper {

    public static List<NestedField> fieldPath(Schema schema, int... idPath) {
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

    private static IllegalArgumentException idPathNotFound(int[] id, List<NestedField> context) {
        final String contextStr = context.stream().map(NestedField::name).collect(Collectors.joining("', '", "['", "']"));
        throw new IllegalArgumentException(String.format("id path not found, path=%s, context=%s", Arrays.toString(id), contextStr));
    }

    private static IllegalArgumentException idPathTooLong(int[] id, List<NestedField> context) {
        final String contextStr = context.stream().map(NestedField::name).collect(Collectors.joining("', '", "['", "']"));
        throw new IllegalArgumentException(String.format("id path too long, path=%s, context=%s", Arrays.toString(id), contextStr));
    }
}
