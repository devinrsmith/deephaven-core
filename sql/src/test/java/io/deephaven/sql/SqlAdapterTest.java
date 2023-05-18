package io.deephaven.sql;

import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.Graphviz;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SqlAdapterTest {

    private static final TableHeader AUTHORS = TableHeader.of(
            ColumnHeader.ofInt("Id"),
            ColumnHeader.ofString("Name"));

    private static final TableHeader BOOKS = TableHeader.of(
            ColumnHeader.ofInt("Id"),
            ColumnHeader.ofString("Title"),
            ColumnHeader.ofInt("AuthorId"));

    private static final TableHeader LONG_I = TableHeader.of(ColumnHeader.ofLong("I"));

    private static final TableHeader TIB = TableHeader.of(
            ColumnHeader.ofInstant("Timestamp"),
            ColumnHeader.ofLong("I"),
            ColumnHeader.ofBoolean("B"));

    private static final TableHeader TIR = TableHeader.of(
            ColumnHeader.ofInstant("Timestamp"),
            ColumnHeader.ofLong("I"),
            ColumnHeader.ofDouble("R"));

    private static final TableHeader TIME1 = TableHeader.of(ColumnHeader.ofInstant("Time1"));

    private static final TableHeader TIME2 = TableHeader.of(ColumnHeader.ofInstant("Time2"));

    @Test
    void sql1() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(
                List.of("AUTHORS"), AUTHORS,
                List.of("BOOKS"), BOOKS);
        check(schema, 1);
    }

    @Test
    void sql2() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(
                List.of("AUTHORS"), AUTHORS,
                List.of("BOOKS"), BOOKS);
        check(schema, 2);
    }

    @Test
    void sql3() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("BOOKS"), BOOKS);
        check(schema, 3);
    }

    @Test
    void sql4() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("BOOKS"), BOOKS);
        check(schema, 4);
    }

    @Test
    void sql5() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("BOOKS"), BOOKS);
        check(schema, 5);
    }

    @Test
    void sql6() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("LONGI"), LONG_I);
        check(schema, 6);
    }

    @Test
    void sql7() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("LONGI"), LONG_I);
        check(schema, 7);
    }

    @Test
    void sql8() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(
                List.of("AUTHORS"), AUTHORS,
                List.of("BOOKS"), BOOKS);
        check(schema, 8);
    }

    @Test
    void sql9() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("BOOKS"), BOOKS);
        check(schema, 9);
    }

    @Test
    void sql10() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("BOOKS"), BOOKS);
        check(schema, 10);
    }

    @Test
    void sql11() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("BOOKS"), BOOKS);
        check(schema, 11);
    }

    @Test
    void sql12() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("BOOKS"), BOOKS);
        check(schema, 12);
    }

    @Test
    void sql13() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("BOOKS"), BOOKS);
        check(schema, 13);
    }

    @Test
    void sql14() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("BOOKS"), BOOKS);
        check(schema, 14);
    }

    @Test
    void sql15() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("BOOKS"), BOOKS);
        check(schema, 15);
    }

    @Test
    void sql16() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("MY_TIME"), TIB);
        check(schema, 16);
    }

    @Test
    void sql17() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(
                List.of("time_1"), TIR,
                List.of("time_2"), TIR,
                List.of("time_3"), TIR);
        check(schema, 17);
    }

    @Test
    void sql18() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("my_time"), TIB);
        check(schema, 18);
    }

    @Test
    void sql19() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(
                List.of("my_time_1"), TIME1,
                List.of("my_time_2"), TIME2);
        check(schema, 19);
    }

    @Test
    void sql20() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(
                List.of("my_time_1"), TIME1,
                List.of("my_time_2"), TIME2);
        check(schema, 20);
    }

    @Test
    void sql21() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Collections.emptyMap();
        check(schema, 21);
    }

    @Test
    void sql22() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Collections.emptyMap();
        check(schema, 22);
    }

    @Test
    void sql23() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Collections.emptyMap();
        check(schema, 23);
    }

    @Test
    void sql24() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Collections.emptyMap();
        check(schema, 24);
    }

    @Test
    void sql25() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("my_time"), TIB);
        check(schema, 25);
    }

    @Test
    void sql26() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("my_time"), TIB);
        check(schema, 26);
    }

    @Test
    void sql27() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("my_time"), TIB);
        check(schema, 27);
    }

    @Test
    void sql28() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(
                List.of("AUTHORS"), AUTHORS,
                List.of("BOOKS"), BOOKS);
        check(schema, 28);
    }

    @Test
    void sql29() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(
                List.of("AUTHORS"), AUTHORS,
                List.of("BOOKS"), BOOKS);
        check(schema, 29);
    }

    @Test
    void sql30() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("BOOKS"), BOOKS);
        check(schema, 30);
    }

    @Test
    void sql31() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("BOOKS"), BOOKS);
        check(schema, 31);
    }

    @Test
    void sql32() throws IOException, URISyntaxException {
        final Map<List<String>, TableHeader> schema = Map.of(List.of("BOOKS"), BOOKS);
        check(schema, 32);
    }

    private static void check(Map<List<String>, TableHeader> schema, int index) throws IOException, URISyntaxException {
        check(schema, String.format("query-%d.sql", index), String.format("qst-%d.dot", index));
    }

    private static void check(Map<List<String>, TableHeader> schema, String queryResource, String expectedResource)
            throws IOException, URISyntaxException {
        checkSql(expectedResource, read(queryResource), schema);
    }

    private static void checkSql(String expectedResource, String sql, Map<List<String>, TableHeader> schema)
            throws IOException, URISyntaxException {
        final TableSpec results = SqlAdapter.parseSql(sql, schema);
        assertThat(Graphviz.toDot(results)).isEqualTo(read(expectedResource));
        // Note: we are *abusing* toDot() / postOrderWalk(), as we are assuming a stable order but the docs specifically
        // say not to do that. Since we control the implementation, we can change the implementation to leverage the
        // "stability" until we have a proper ser/deser format (json?) that we can use for TableSpec. The alternative is
        // to manually create the TableSpecs in-code - which is possible, but would be tedious and verbose.
        //
        // Additionally, the dot format is somewhat human readable, and provides a good avenue for visualization
        // purposes during the development and testing of SqlAdapter.
        //
        // assertThat(results).isEqualTo(readJsonToTableSpec(expectedResource));
    }

    private static String read(String resourceName) throws IOException, URISyntaxException {
        return Files.readString(Path.of(SqlAdapterTest.class.getResource(resourceName).toURI()));
    }
}
