package io.deephaven.db.tables.utils;

import io.deephaven.qst.Column;
import io.deephaven.qst.Column.ColumnBuilder;
import io.deephaven.qst.ColumnHeader;
import io.deephaven.qst.ColumnType.Visitor;
import io.deephaven.qst.DoubleType;
import io.deephaven.qst.GenericType;
import io.deephaven.qst.IntType;
import io.deephaven.qst.NewTable;
import io.deephaven.qst.StringType;
import io.deephaven.qst.TableHeader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CsvQst {

    public static <T> Column<T> parse(ColumnHeader<T> header, Iterable<String> values) {
        //noinspection unchecked
        return (Column<T>) header.type().walk(new ToColumn(header, values)).getOut();
    }

    public static NewTable parse(TableHeader tableHeader, File file) throws IOException {
        try (final CSVParser parser = CSVFormat.DEFAULT
            //.withSkipHeaderRecord(true)
            .parse(new FileReader(file))) {
            List<CSVRecord> records = parser.getRecords();
            return parse(tableHeader, records.subList(1, records.size())); // todo: skip header doesn't seem to work w/ getRecords()
        }
    }

    private static NewTable parse(TableHeader tableHeader, Iterable<CSVRecord> records) {
        final int L = tableHeader.numColumns();
        final List<Column<?>> columns = new ArrayList<>(L);
        for (int i = 0; i < L; ++i) {
            final int index = i;
            final Iterable<String> values = () -> new Iterator<String>() {
                private final Iterator<CSVRecord> it = records.iterator();

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public String next() {
                    return it.next().get(index);
                }
            };
            columns.add(parse(tableHeader.headers().get(i), values));
        }
        return NewTable.of(columns);
    }

    private static class ToColumn implements Visitor {
        private final ColumnHeader<?> header;
        private final Iterable<String> stringValues;
        private Column<?> out;

        ToColumn(ColumnHeader<?> header, Iterable<String> stringValues) {
            this.header = Objects.requireNonNull(header);
            this.stringValues = Objects.requireNonNull(stringValues);
        }

        public Column<?> getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(IntType intType) {
            ColumnBuilder<Integer> builder = intType.cast(header).withBuilder();
            for (String value : stringValues) {
                builder.add(parseInt(value));
            }
            out = builder.build();
        }

        @Override
        public void visit(StringType stringType) {
            ColumnBuilder<String> builder = stringType.cast(header).withBuilder();
            for (String value : stringValues) {
                builder.add(parseString(value));
            }
            out = builder.build();
        }

        @Override
        public void visit(DoubleType doubleType) {
            ColumnBuilder<Double> builder = doubleType.cast(header).withBuilder();
            for (String value : stringValues) {
                builder.add(parseDouble(value));
            }
            out = builder.build();
        }

        @Override
        public void visit(GenericType<?> genericType) {
            throw new IllegalArgumentException("Don't support generic type yet.");
        }
    }

    // todo: parameterize how they are parsed?

    private static Integer parseInt(String value) {
        if (value.isEmpty()) {
            return null;
        }
        return Integer.parseInt(value);
    }

    private static Double parseDouble(String value) {
        if (value.isEmpty()) {
            return null;
        }
        return Double.parseDouble(value);
    }

    private static String parseString(String value) {
        if (value.isEmpty()) {
            return null; // todo: provide way for user to specify we should parse empty as empty?
        }
        return value;
    }
}
