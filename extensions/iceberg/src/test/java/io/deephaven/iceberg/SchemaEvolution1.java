package io.deephaven.iceberg;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.sqlite.DbResource;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import io.deephaven.iceberg.util.IcebergTable;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.iceberg.util.SchemaProvider;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("security-manager-allow")
public class SchemaEvolution1 {

    private static final TableIdentifier TABLE_ID = TableIdentifier.of("schema-evolution", "test-55");

    private IcebergTableAdapter tableAdapter;
    private int id1;
    private int id2;

    @BeforeEach
    void setUp() throws URISyntaxException {
        tableAdapter = DbResource.openCatalog("schema-evolution").loadTable(TABLE_ID);
        {
            final Schema initialSchema = tableAdapter.schema(0).orElseThrow();
            id1 = initialSchema.findField("Field1").fieldId();
            id2 = initialSchema.findField("Field2").fieldId();
        }
    }

    @Test
    void schemas() {
        final Map<Integer, Schema> schemas = tableAdapter.schemas();
        assertThat(schemas).hasSize(4);
        assertThat(schemas).extractingByKey(0).usingEquals(Schema::sameSchema).isEqualTo(schema_0());
        assertThat(schemas).extractingByKey(1).usingEquals(Schema::sameSchema).isEqualTo(schema_1());
        assertThat(schemas).extractingByKey(2).usingEquals(Schema::sameSchema).isEqualTo(schema_2());
        assertThat(schemas).extractingByKey(3).usingEquals(Schema::sameSchema).isEqualTo(schema_3());
    }

    @Test
    void currentSchema() {
        assertThat(tableAdapter.currentSchema()).usingEquals(Schema::sameSchema).isEqualTo(schema_3());
    }

    @Test
    void tableFromCurrent() {
        final TableDefinition def = TableDefinition.of(
                ColumnDefinition.ofInt("Field2_C"),
                ColumnDefinition.ofInt("Field1_C"));
        final Table expected = TableTools.newTable(def,
                TableTools.intCol("Field2_C", data(60, true)),
                TableTools.intCol("Field1_C", data(60, false)));
        final Table actual = tableAdapter.table(IcebergReadInstructions.builder()
                .schema(SchemaProvider.fromCurrent())
                .build());
        TstUtils.assertTableEquals(expected, actual);
    }

    private static int[] data(int size, boolean neg) {
        int[] data = new int[size];
        for (int i = 0; i < size; i++) {
            data[i] = neg ? -i : i;
        }
        return data;
    }

    private Schema schema_0() {
        return new Schema(
                Types.NestedField.of(id1, true, "Field1", IntegerType.get()),
                Types.NestedField.of(id2, true, "Field2", IntegerType.get()));
    }

    private Schema schema_1() {
        return new Schema(
                Types.NestedField.of(id1, true, "Field1_B", IntegerType.get()),
                Types.NestedField.of(id2, true, "Field2_B", IntegerType.get()));
    }

    private Schema schema_2() {
        return new Schema(
                Types.NestedField.of(id1, true, "Field1_C", IntegerType.get()),
                Types.NestedField.of(id2, true, "Field2_C", IntegerType.get()));
    }

    private Schema schema_3() {
        return new Schema(
                Types.NestedField.of(id2, true, "Field2_C", IntegerType.get()),
                Types.NestedField.of(id1, true, "Field1_C", IntegerType.get()));
    }
}
