package io.deephaven.iceberg.layout;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

class SchemaHelperTest {

    @Test
    void nestedFields() {
        final NestedField i1 = NestedField.of(1, true, "I1", IntegerType.get());
        final NestedField i2 = NestedField.of(2, true, "I2", IntegerType.get());
        final NestedField i3 = NestedField.of(3, true, "I3", IntegerType.get());
        final NestedField i4 = NestedField.of(4, true, "I4", StructType.of(i2, i3));
        final NestedField i6 = NestedField.of(6, true, "I6", ListType.ofOptional(5, IntegerType.get()));
        final NestedField i9 = NestedField.of(9, true, "I9", MapType.ofOptional(7, 8, IntegerType.get(), IntegerType.get()));
        final Schema schema = new Schema(i1, i4, i6, i9);

        assertNestedFields(schema).isEmpty();

        assertNestedFields(schema, 1).containsExactly("I1");
        assertNestedFieldsError("Invalid path, is too long", schema, 1, 2);

        assertNestedFields(schema, 4, 2).containsExactly("I4", "I2");
        assertNestedFields(schema, 4, 3).containsExactly("I4", "I3");
        assertNestedFieldsError("Invalid id path, id=1 @ ix=1", schema, 4, 1);
        assertNestedFieldsError("Invalid path, is too long", schema, 4, 2, 1);

        assertNestedFields(schema, 6).containsExactly("I6");
        assertNestedFields(schema, 6, 5).containsExactly("I6", "element");
        assertNestedFieldsError("Invalid id path, id=4 @ ix=1", schema, 6, 4);
        assertNestedFieldsError("Invalid path, is too long", schema, 6, 5, 4);

        assertNestedFields(schema, 9).containsExactly("I9");
        assertNestedFields(schema, 9, 7).containsExactly("I9", "key");
        assertNestedFields(schema, 9, 8).containsExactly("I9", "value");
        assertNestedFieldsError("Invalid id path, id=6 @ ix=1", schema, 9, 6);
        assertNestedFieldsError("Invalid path, is too long", schema, 9, 7, 6);
    }

    private static ListAssert<String> assertNestedFields(Schema schema, int... values) {
        return assertThat(SchemaHelper.nestedFields(schema, IntStream.of(values).iterator()).map(NestedField::name));
    }

    private static void assertNestedFieldsError(String message, Schema schema, int... values) {
        try {
            SchemaHelper.nestedFields(schema, IntStream.of(values).iterator()).forEach(x -> {});
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining(message);
        }
    }
}
