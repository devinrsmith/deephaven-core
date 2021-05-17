package io.deephaven.datastructures.util;

import org.junit.Assert;
import org.junit.Test;

public class NewTableTest {

    @Test
    public void checkColumnSize() {
        try {
            NewTable.of(
                Column.of("Size1", 1),
                Column.of("Size2", 1, 2));
            Assert.fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void checkDistinctNames() {
        try {
            NewTable.of(
                Column.of("Size1", 1),
                Column.of("Size1", 2));
            Assert.fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void newTableHelperColumnOriented() {
        NewTable expected = ImmutableNewTable.builder()
            .addColumns(ImmutableColumn.<Integer>builder()
                .header(ImmutableColumnHeader.<Integer>builder()
                    .name("X")
                    .type(IntType.instance())
                    .build())
                .addValues(1, null, 3)
                .build())
            .build();

        NewTable actual = NewTable.of(Column.of("X", 1, null, 3));

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void newTableHelperRowOriented() {

        Column<Integer> x = ImmutableColumn.<Integer>builder()
            .header(ImmutableColumnHeader.<Integer>builder()
                .name("X")
                .type(IntType.instance())
                .build())
            .addValues(1, 2)
            .build();

        Column<String> y = ImmutableColumn.<String>builder()
            .header(ImmutableColumnHeader.<String>builder()
                .name("Y")
                .type(StringType.instance())
                .build())
            .addValues("one", "two")
            .build();

        NewTable expected = ImmutableNewTable.builder()
            .addColumns(x, y)
            .build();

        NewTable actual = NewTable
            .header("X", int.class)
            .header("Y", String.class)
            .row(1, "one")
            .row(2, "two")
            .build();

        Assert.assertEquals(expected, actual);
    }

}
