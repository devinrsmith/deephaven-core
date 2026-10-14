//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.vector.ObjectVector;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link TableDefinition#checkMutualCompatibility(Map)}, which must behave exactly like
 * {@code checkMutualCompatibility(TableDefinition.inferFrom(columns))}, but without creating definitions when the
 * column sources are compatible and in order.
 */
public class TableDefinitionSourceCompatibilityTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("Partition").withPartitioning(),
            ColumnDefinition.ofInt("Int"),
            ColumnDefinition.ofBoolean("Bool"),
            ColumnDefinition.ofTime("Time"),
            ColumnDefinition.fromGenericType("Instants", Instant[].class),
            ColumnDefinition.ofVector("Strings", ObjectVector.class, String.class));

    private static Map<String, ColumnSource<?>> sources() {
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        columns.put("Partition", ArrayBackedColumnSource.getMemoryColumnSource(String.class, null));
        columns.put("Int", ArrayBackedColumnSource.getMemoryColumnSource(int.class, null));
        columns.put("Bool", ArrayBackedColumnSource.getMemoryColumnSource(Boolean.class, null));
        columns.put("Time", ArrayBackedColumnSource.getMemoryColumnSource(Instant.class, null));
        columns.put("Instants", ArrayBackedColumnSource.getMemoryColumnSource(Instant[].class, null));
        columns.put("Strings", ArrayBackedColumnSource.getMemoryColumnSource(ObjectVector.class, String.class));
        return columns;
    }

    @Test
    public void compatibleInOrderReturnsThis() {
        final Map<String, ColumnSource<?>> columns = sources();
        // No definition is created: this exact instance comes back, partitioning column type and all
        assertSame(DEFINITION, DEFINITION.checkMutualCompatibility(columns));
        assertEquals(slowPath(DEFINITION, columns), DEFINITION.checkMutualCompatibility(columns));
    }

    @Test
    public void compatibleReorderedMatchesSlowPath() {
        final Map<String, ColumnSource<?>> reordered = new LinkedHashMap<>();
        final Map<String, ColumnSource<?>> columns = sources();
        reordered.put("Time", columns.get("Time"));
        reordered.put("Int", columns.get("Int"));
        columns.forEach(reordered::putIfAbsent);

        final TableDefinition result = DEFINITION.checkMutualCompatibility(reordered);
        assertNotSame(DEFINITION, result);
        assertEquals(slowPath(DEFINITION, reordered), result);
        assertEquals("Time", result.getColumns().get(0).getName());
        assertTrue(result.getColumn("Partition").isPartitioning());
    }

    @Test
    public void incompatibleDataType() {
        final Map<String, ColumnSource<?>> columns = sources();
        columns.put("Int", ArrayBackedColumnSource.getMemoryColumnSource(long.class, null));
        assertSameFailure(DEFINITION, columns);
    }

    @Test
    public void incompatibleComponentType() {
        // Same data type, different element type: ObjectVector<Object> is not ObjectVector<String>
        final Map<String, ColumnSource<?>> columns = sources();
        columns.put("Strings", ArrayBackedColumnSource.getMemoryColumnSource(ObjectVector.class, Object.class));
        assertSameFailure(DEFINITION, columns);
    }

    @Test
    public void incompatibleArrayType() {
        final Map<String, ColumnSource<?>> columns = sources();
        columns.put("Instants", ArrayBackedColumnSource.getMemoryColumnSource(Object[].class, null));
        assertSameFailure(DEFINITION, columns);
    }

    @Test
    public void missingAndExtraColumns() {
        final Map<String, ColumnSource<?>> missing = sources();
        missing.remove("Time");
        assertSameFailure(DEFINITION, missing);

        final Map<String, ColumnSource<?>> extra = sources();
        extra.put("Extra", ArrayBackedColumnSource.getMemoryColumnSource(int.class, null));
        assertSameFailure(DEFINITION, extra);

        final Map<String, ColumnSource<?>> renamed = new LinkedHashMap<>();
        sources().forEach((name, source) -> renamed.put(name.equals("Int") ? "Renamed" : name, source));
        assertSameFailure(DEFINITION, renamed);
    }

    @Test
    public void queryTableConstructorReusesDefinition() {
        final QueryTable table = new QueryTable(DEFINITION, RowSetFactory.empty().toTracking(), sources());
        assertSame(DEFINITION.intern(), table.getDefinition());
        assertTrue(table.getDefinition().getColumn("Partition").isPartitioning());
    }

    private static TableDefinition slowPath(
            final TableDefinition definition,
            final Map<String, ColumnSource<?>> columns) {
        return definition.checkMutualCompatibility(TableDefinition.inferFrom(columns));
    }

    private static void assertSameFailure(
            final TableDefinition definition,
            final Map<String, ColumnSource<?>> columns) {
        final RuntimeException expected = assertThrows(RuntimeException.class, () -> slowPath(definition, columns));
        final RuntimeException actual =
                assertThrows(RuntimeException.class, () -> definition.checkMutualCompatibility(columns));
        assertEquals(expected.getClass(), actual.getClass());
        assertEquals(expected.getMessage(), actual.getMessage());
    }
}
