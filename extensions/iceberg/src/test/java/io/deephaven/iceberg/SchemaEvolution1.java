package io.deephaven.iceberg;

import io.deephaven.iceberg.sqlite.DbResource;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.parquet.table.location.ParquetColumnResolver;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("security-manager-allow")
public class SchemaEvolution1 {

    private static final TableIdentifier TABLE_ID = TableIdentifier.of("schema-evolution", "test-1");
    private static final int FIELD_ID_1 = 1;
    private static final int FIELD_ID_2 = 2;

    private static final Schema SCHEMA_0 = new Schema(
            Types.NestedField.of(FIELD_ID_1, true, "Field1", IntegerType.get()),
            Types.NestedField.of(FIELD_ID_2, true, "Field2", IntegerType.get()));

    private static final Schema SCHEMA_1 = new Schema(
            Types.NestedField.of(FIELD_ID_1, true, "Field1_B", IntegerType.get()),
            Types.NestedField.of(FIELD_ID_2, true, "Field2_B", IntegerType.get()));

    private static final Schema SCHEMA_2 = new Schema(
            Types.NestedField.of(FIELD_ID_1, true, "Field1_C", IntegerType.get()),
            Types.NestedField.of(FIELD_ID_2, true, "Field2_C", IntegerType.get()));

    private static final Schema SCHEMA_3 = new Schema(
            Types.NestedField.of(FIELD_ID_2, true, "Field2_C", IntegerType.get()),
            Types.NestedField.of(FIELD_ID_1, true, "Field1_C", IntegerType.get()));

    private IcebergTableAdapter tableAdapter;

    @BeforeEach
    void setUp() throws URISyntaxException {
        tableAdapter = DbResource.openCatalog("schema-evolution").loadTable(TABLE_ID);
    }

    @Test
    void schemas() {
        final Map<Integer, Schema> schemas = tableAdapter.schemas();
        assertThat(schemas).hasSize(4);
        assertThat(schemas).extractingByKey(0).usingEquals(Schema::sameSchema).isEqualTo(SCHEMA_0);
        assertThat(schemas).extractingByKey(1).usingEquals(Schema::sameSchema).isEqualTo(SCHEMA_1);
        assertThat(schemas).extractingByKey(2).usingEquals(Schema::sameSchema).isEqualTo(SCHEMA_2);
        assertThat(schemas).extractingByKey(3).usingEquals(Schema::sameSchema).isEqualTo(SCHEMA_3);
    }

    @Test
    void currentSchema() {
        assertThat(tableAdapter.currentSchema()).usingEquals(Schema::sameSchema).isEqualTo(SCHEMA_3);
    }

    @Test
    void name() {

        ParquetColumnResolver resolver = new ParquetColumnResolver() {
            @Override
            public Optional<List<String>> of(String columnName) {
                return Optional.empty();
            }
        };

        tableAdapter.table(IcebergReadInstructions.builder().build());

    }
}
