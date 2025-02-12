package io.deephaven.iceberg.internal;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * testing mapping is tougher than testing inference because it needs to deal with type conversion logic
 */
class MappingTest {

    private static final IntegerType IT = IntegerType.get();

    @Test
    void noSuchColumn() {
        try {
            Mapping.builder()
                    .schema(simpleSchema(IT))
                    .definition(simpleDefinition(Type.intType()))
                    .putPath("F1", List.of(42))
                    .putPath("F2", List.of(43))
                    .putPath("F3", List.of(42))
                    .build();
        } catch (NoSuchColumnException e) {
            assertThat(e).hasMessageContaining("Unknown column names [F3], available column names are [F1, F2]");
        }
    }

    @Test
    void noSuchPath() {
        try {
            Mapping.builder()
                    .schema(simpleSchema(IT))
                    .definition(simpleDefinition(Type.intType()))
                    .putPath("F1", List.of(42))
                    .putPath("F2", List.of(44))
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("id path not found, path=[44], context=['']");
        }
    }

    @Test
    void unmappedColumn() {
        // All DH columns must be mapped by default.
        try {
            Mapping.builder()
                    .schema(simpleSchema(IT))
                    .definition(simpleDefinition(Type.intType()))
                    .putPath("F1", List.of(42))
                    .build();
            failBecauseExceptionWasNotThrown(Mapping.MappingException.class);
        } catch (Mapping.MappingException e) {
            assertThat(e).hasMessageContaining("Column `F2` is not mapped");
        }
        // But, there is support to allow them, which is necessary for cases where a Schema field has been deleted, but
        // we want to keep the column in DH
        Mapping.builder()
                .schema(simpleSchema(IT))
                .definition(simpleDefinition(Type.intType()))
                .putPath("F1", List.of(42))
                .allowUnmappedColumns(true)
                .build();
    }

    @Test
    void duplicateMapping() {
        // It's okay to map the same Iceberg field to different DH columns
        Mapping.builder()
                .schema(simpleSchema(IT))
                .definition(TableDefinition.of(
                        ColumnDefinition.of("F1", Type.intType()),
                        ColumnDefinition.of("F2", Type.intType()),
                        ColumnDefinition.of("F3", Type.intType())))
                .putPath("F1", List.of(42))
                .putPath("F2", List.of(43))
                .putPath("F3", List.of(43))
                .build();

    }

    @Test
    void unmappedIcebergField() {
        // It's okay to not map all the Iceberg fields
        Mapping.builder()
                .schema(new Schema(
                        NestedField.optional(42, "F1", IT),
                        NestedField.required(43, "F2", IT),
                        NestedField.required(44, "F3", IT)))
                .definition(simpleDefinition(Type.intType()))
                .putPath("F1", List.of(42))
                .putPath("F2", List.of(43))
                .build();
    }

    @Test
    void invalidMappingType() {
        // TODO: we should try to be thorough in describing what we do and do not support
        try {
            Mapping.builder()
                    .schema(simpleSchema(IT))
                    .definition(simpleDefinition(Type.stringType()))
                    .putPath("F1", List.of(42))
                    .putPath("F2", List.of(43))
                    .build();
            failBecauseExceptionWasNotThrown(Mapping.MappingException.class);
        } catch (Mapping.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Iceberg type `int` to Deephaven type `io.deephaven.qst.type.StringType`");
        }
    }

    //    @Test
//    void simpleSubset() {
//        final Mapping mapping = Mapping.builder()
//                .schema(simpleSchema(IT))
//                .definition(simpleDefinition(Type.intType()))
//                .putPath("F1", List.of(42))
//                .build();
//    }
//
//    @Test
//    void name() {
//        Mapping.builder()
//                .schema(simpleSchema(IT))
//                .definition(TableDefinition.of(
//                        ColumnDefinition.ofShort("F1"),
//                        ColumnDefinition.ofShort("F2")))
//                .putPath("F1", List.of(42))
//                .putPath("F2", List.of(43))
//                .build();
//    }
//
//    @Test
//    void ListType() {
//
//        final Schema schema = new Schema(NestedField.optional(2, "L1", Types.ListType.ofOptional(1, IT)));
//
//        Mapping.builder()
//                .schema(schema)
//                .definition(TableDefinition.of(ColumnDefinition.of("L1", Type.intType().arrayType())))
//                .putPath("L1", List.of(2))
//                .build();
//    }

    private static Schema simpleSchema(org.apache.iceberg.types.Type type) {
        return new Schema(
                NestedField.optional(42, "F1", type),
                NestedField.required(43, "F2", type));
    }

    private static TableDefinition simpleDefinition(Type<?> type) {
        return TableDefinition.of(
                ColumnDefinition.of("F1", type),
                ColumnDefinition.of("F2", type));
    }
}
