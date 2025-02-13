//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.internal.Inference;
import io.deephaven.iceberg.sqlite.DbResource;
import io.deephaven.iceberg.util.DefinitionInstructions;
import io.deephaven.iceberg.util.FieldPath;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import io.deephaven.iceberg.util.IcebergTableAdapter;
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

    private static final TableDefinition IDEF_0 = TableDefinition.of(
            ColumnDefinition.ofInt("Field1"),
            ColumnDefinition.ofInt("Field2"));

    private static final TableDefinition IDEF_1 = TableDefinition.of(
            ColumnDefinition.ofInt("Field1_B"),
            ColumnDefinition.ofInt("Field2_B"));

    private static final TableDefinition IDEF_2 = TableDefinition.of(
            ColumnDefinition.ofInt("Field1_C"),
            ColumnDefinition.ofInt("Field2_C"));

    private static final TableDefinition IDEF_3 = TableDefinition.of(
            ColumnDefinition.ofInt("Field2_C"),
            ColumnDefinition.ofInt("Field1_C"));

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
        final Table actual = tableAdapter.table();
        TstUtils.assertTableEquals(expected, actual);
    }

    @Test
    void readLatestAs() throws Inference.Exception {
        read(expected(IDEF_0, 60, false), readLatestAs(0));
        read(expected(IDEF_1, 60, false), readLatestAs(1));
        read(expected(IDEF_2, 60, false), readLatestAs(2));
        read(expected(IDEF_3, 60, true), readLatestAs(3));
    }

    @Test
    void readSnapshot5As() throws Inference.Exception {
        read(expected(IDEF_0, 60, false), readSnapshotAs(5, 0));
        read(expected(IDEF_1, 60, false), readSnapshotAs(5, 1));
        read(expected(IDEF_2, 60, false), readSnapshotAs(5, 2));
        read(expected(IDEF_3, 60, true), readSnapshotAs(5, 3));
    }

    @Test
    void readSnapshot4As() throws Inference.Exception {
        read(expected(IDEF_0, 50, false), readSnapshotAs(4, 0));
        read(expected(IDEF_1, 50, false), readSnapshotAs(4, 1));
        read(expected(IDEF_2, 50, false), readSnapshotAs(4, 2));
        read(expected(IDEF_3, 50, true), readSnapshotAs(4, 3));
    }

    @Test
    void readSnapshot3As() throws Inference.Exception {
        read(expected(IDEF_0, 40, false), readSnapshotAs(3, 0));
        read(expected(IDEF_1, 40, false), readSnapshotAs(3, 1));
        read(expected(IDEF_2, 40, false), readSnapshotAs(3, 2));
        read(expected(IDEF_3, 40, true), readSnapshotAs(3, 3));
    }

    @Test
    void readSnapshot2As() throws Inference.Exception {
        read(expected(IDEF_0, 30, false), readSnapshotAs(2, 0));
        read(expected(IDEF_1, 30, false), readSnapshotAs(2, 1));
        read(expected(IDEF_2, 30, false), readSnapshotAs(2, 2));
        read(expected(IDEF_3, 30, true), readSnapshotAs(2, 3));
    }

    @Test
    void readSnapshot1As() throws Inference.Exception {
        read(expected(IDEF_0, 20, false), readSnapshotAs(1, 0));
        read(expected(IDEF_1, 20, false), readSnapshotAs(1, 1));
        read(expected(IDEF_2, 20, false), readSnapshotAs(1, 2));
        read(expected(IDEF_3, 20, true), readSnapshotAs(1, 3));
    }

    @Test
    void readSnapshot0As() throws Inference.Exception {
        read(expected(IDEF_0, 10, false), readSnapshotAs(0, 0));
        read(expected(IDEF_1, 10, false), readSnapshotAs(0, 1));
        read(expected(IDEF_2, 10, false), readSnapshotAs(0, 2));
        read(expected(IDEF_3, 10, true), readSnapshotAs(0, 3));
    }

    @Test
    void customDefinitions() {
        // subset, just id1
        {
            final String col1 = "Foo";
            final Table expected = TableTools.newTable(
                    TableDefinition.of(ColumnDefinition.ofInt(col1)),
                    TableTools.intCol(col1, data(60, false)));
            read(expected, DefinitionInstructions.builder()
                    .schema(schema_0())
                    .definition(TableDefinition.of(ColumnDefinition.ofInt(col1)))
                    .putColumnInstructions(col1, FieldPath.of(id1))
                    .build());
        }
        // subset, just id2
        {
            final String col2 = "Bar";
            final Table expected = TableTools.newTable(
                    TableDefinition.of(ColumnDefinition.ofInt(col2)),
                    TableTools.intCol(col2, data(60, true)));
            read(expected, DefinitionInstructions.builder()
                    .schema(schema_0())
                    .definition(TableDefinition.of(ColumnDefinition.ofInt(col2)))
                    .putColumnInstructions(col2, FieldPath.of(id2))
                    .build());
        }
//        // superset
//        {
//            final String col1 = "Foo";
//            final String col2 = "Bar";
//            final Table expected = TableTools.newTable(
//                    TableDefinition.of(
//                            ColumnDefinition.ofInt(int)
//                            ColumnDefinition.ofInt(col2)),
//                    TableTools.intCol(col1, data(60, false)),
//                    TableTools.intCol(col2, data(60, true)));
//            read(expected, DefinitionInstructions.builder()
//                    .schema(schema_0())
//                    .definition(TableDefinition.of(ColumnDefinition.ofInt(col2)))
//                    .putColumnInstructions(col2, FieldPath.of(id2))
//                    .build());
//        }
    }

    private void read(Table expected, DefinitionInstructions di) {
        read(expected, IcebergReadInstructions.builder().definitionInstructions(di).build());
    }

    private void read(Table expected, IcebergReadInstructions instructions) {
        assertThat(tableAdapter.definition(instructions)).isEqualTo(expected.getDefinition());
        TstUtils.assertTableEquals(expected, tableAdapter.table(instructions));
    }

    private static Table expected(TableDefinition td, int size, boolean swapped) {
        return TableTools.newTable(td,
                TableTools.intCol(td.getColumnNames().get(0), data(size, swapped)),
                TableTools.intCol(td.getColumnNames().get(1), data(size, !swapped)));
    }

    private static int[] data(int size, boolean neg) {
        int[] data = new int[size];
        for (int i = 0; i < size; i++) {
            data[i] = neg ? -i : i;
        }
        return data;
    }

    private IcebergReadInstructions readLatestAs(int schemaVersion) throws Inference.Exception {
        return IcebergReadInstructions.builder()
                .definitionInstructions(DefinitionInstructions.inferAll(schema(schemaVersion)))
                .build();
    }

    private IcebergReadInstructions readSnapshotAs(int snapshotIx, int schemaVersion) throws Inference.Exception {
        return IcebergReadInstructions.builder()
                .snapshot(tableAdapter.listSnapshots().get(snapshotIx))
                .definitionInstructions(DefinitionInstructions.inferAll(schema(schemaVersion)))
                .build();
    }

    private Schema schema(int version) {
        switch (version) {
            case 0: return schema_0();
            case 1: return schema_1();
            case 2: return schema_2();
            case 3: return schema_3();
            default:
                throw new IllegalStateException();
        }
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
