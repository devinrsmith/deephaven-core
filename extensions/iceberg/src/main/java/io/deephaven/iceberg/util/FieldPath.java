//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.iceberg.internal.SchemaHelper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;
import org.immutables.value.Value;

import java.util.List;


/**
 * A path into a {@link Schema}.
 */
@Value.Immutable
@SimpleStyle
public abstract class FieldPath {

    /**
     * Creates a field path that resolves fields via {@link NestedField#fieldId()}.
     *
     * @param fieldIdPath the field id path
     * @return the field path
     */
    public static FieldPath of(int... fieldIdPath) {
        return ImmutableFieldPath.of(fieldIdPath);
    }

    /**
     * Creates a field path that resolves fields via {@link NestedField#fieldId()}}. This method ensures that the field
     * id path exists in {@code schema}.
     *
     * @param schema the schema
     * @param fieldIdPath the field id path
     * @return the field path
     */
    public static FieldPath of(Schema schema, int... fieldIdPath) throws SchemaHelper.PathException {
        // Check that the fieldIdPath is resolvable
        SchemaHelper.fieldPath(schema, fieldIdPath); // todo
        return of(fieldIdPath);
    }


    /**
     * Creates a field path that resolves fields via {@link NestedField#name()}. This method ensures that the field name
     * path exists in {@code schema}.
     *
     * @param schema the schema
     * @param fieldNamePath the field name path
     * @return the field path
     */
    public static FieldPath of(Schema schema, String... fieldNamePath) throws SchemaHelper.PathException {
        // todo
        final int[] idPath = SchemaHelper.fieldPath(schema, fieldNamePath)
                .stream()
                .mapToInt(NestedField::fieldId)
                .toArray();
        return of(idPath);
    }

    @Value.Parameter
    abstract int[] path();

    // todo: make package private
    // todo
    final List<NestedField> resolve(Schema schema) throws SchemaHelper.PathException {
        return SchemaHelper.fieldPath(schema, path());
    }

    final boolean isContainedIn(Schema schema) {
        return SchemaHelper.hasFieldPath(schema, path());
    }
}
