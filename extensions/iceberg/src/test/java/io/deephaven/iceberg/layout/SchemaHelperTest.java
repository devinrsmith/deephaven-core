package io.deephaven.iceberg.layout;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Test;

import java.util.List;
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
        assertNestedFieldsError("id path too long, path=[1, 2], context=['I1']", schema, 1, 2);

        assertNestedFields(schema, 4, 2).containsExactly("I4", "I2");
        assertNestedFields(schema, 4, 3).containsExactly("I4", "I3");
        assertNestedFieldsError("id path not found, path=[4, 1], context=['I4']", schema, 4, 1);
        assertNestedFieldsError("id path too long, path=[4, 2, 1], context=['I4', 'I2']", schema, 4, 2, 1);

        assertNestedFields(schema, 6).containsExactly("I6");
        assertNestedFields(schema, 6, 5).containsExactly("I6", "element");
        assertNestedFieldsError("id path not found, path=[6, 4], context=['I6']", schema, 6, 4);
        assertNestedFieldsError("id path too long, path=[6, 5, 4], context=['I6', 'element']", schema, 6, 5, 4);

        assertNestedFields(schema, 9).containsExactly("I9");
        assertNestedFields(schema, 9, 7).containsExactly("I9", "key");
        assertNestedFields(schema, 9, 8).containsExactly("I9", "value");
        assertNestedFieldsError("id path not found, path=[9, 6], context=['I9']", schema, 9, 6);
        assertNestedFieldsError("id path too long, path=[9, 7, 6], context=['I9', 'key']", schema, 9, 7, 6);
        assertNestedFieldsError("id path too long, path=[9, 8, 6], context=['I9', 'value']", schema, 9, 8, 6);
    }

    private static AbstractListAssert<?, List<? extends String>, String, ObjectAssert<String>> assertNestedFields(Schema schema, int... values) {
        return assertThat(SchemaHelper.nestedFields(schema, values)).extracting(NestedField::name);
    }

    private static void assertNestedFieldsError(String message, Schema schema, int... values) {
        try {
            SchemaHelper.nestedFields(schema, values).forEach(x -> {});
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining(message);
        }
    }
}
