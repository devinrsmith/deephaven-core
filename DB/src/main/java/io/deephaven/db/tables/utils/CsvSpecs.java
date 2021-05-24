package io.deephaven.db.tables.utils;

import io.deephaven.qst.Column;
import io.deephaven.qst.ColumnHeader;
import io.deephaven.qst.ColumnType;
import io.deephaven.qst.NewTable;
import io.deephaven.qst.TableHeader;
import io.deephaven.qst.TypeLogic;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class CsvSpecs {

    private static final List<ColumnType<?>> INFER_ORDER = Arrays.asList(
        ColumnType.booleanType(),
        ColumnType.intType(),
        ColumnType.doubleType());

    public abstract Optional<TableHeader> header();

    public abstract Optional<File> file();

    public abstract Optional<URL> url();

    @Default
    public boolean hasHeaderRow() {
        return true;
    }

    @Default
    public char delimiter() {
        return ',';
    }

    @Default
    public char quote() {
        return '"';
    }

    @Default
    public boolean trim() {
        return false;
    }

    @Default
    public Charset charset() {
        return StandardCharsets.UTF_8;
    }

    @Default
    public TypeLogic typeLogic() {
        return null; // todo
    }

    @Check
    final void checkSource() {
        if (file().isPresent()) {
            if (url().isPresent()) {
                throw new IllegalArgumentException();
            }
            return;
        }
        if (url().isPresent()) {
            return;
        }
        throw new IllegalArgumentException();
    }

    private CSVFormat format() {
        return CSVFormat.DEFAULT
          .withDelimiter(delimiter())
          .withQuote(quote())
          .withTrim(trim());
    }

    private Reader reader() throws IOException {
        if (file().isPresent()) {
            // Note: Java 8 FileReader doesn't take charset. This is fixed in Java 11
            //reader = new FileReader(file().get());
            return new InputStreamReader(new FileInputStream(file().get()), charset());
        } else if (url().isPresent()) {
            return new InputStreamReader(url().get().openStream(), charset());
        }
        throw new IllegalStateException();
    }

    public final NewTable parse() throws IOException {
        try (
            final Reader reader = reader();
            final CSVParser parser = format().parse(reader)) {
            final List<CSVRecord> records = parser.getRecords();
            if (hasHeaderRow() && records.isEmpty()) {
                throw new RuntimeException("Expected header row, none found");
            }
            final List<CSVRecord> dataRecords = hasHeaderRow() ?
                records.subList(1, records.size()) :
                records;

            if (!header().isPresent() && dataRecords.isEmpty()) {
                throw new RuntimeException("Unable to infer types with no TableHeader and no data");
            }

            final TableHeader header;
            if (header().isPresent()) {
                header = header().get();
            } else {
                final List<ColumnHeader<?>> columnHeaders = new ArrayList<>();
                final Iterable<String> columnNames;
                if (hasHeaderRow()) {
                    columnNames = records.get(0);
                } else {
                    // todo: this should be safe, given our check earlier about inferring
                    final int numColumns = dataRecords.get(0).size();
                    columnNames = IntStream
                        .range(0, numColumns)
                        .mapToObj(i -> String.format("Column_%d", i))
                        .collect(Collectors.toList());
                }
                int columnIndex = 0;
                for (String columnName : columnNames) {
                    // todo: what about inferring when columns are null?
                    final Iterable<String> columnValues = getColumn(dataRecords, columnIndex);
                    final ColumnType<?> columnType = inferType(columnValues);
                    final ColumnHeader<?> columnHeader = ColumnHeader.of(columnName, columnType);
                    columnHeaders.add(columnHeader);
                    ++columnIndex;
                }
                header = TableHeader.of(columnHeaders);
            }
            return parse(header, dataRecords);
        }
    }

    private NewTable parse(TableHeader tableHeader, Iterable<CSVRecord> records) {
        final int L = tableHeader.numColumns();
        final List<Column<?>> columns = new ArrayList<>(L);
        for (int i = 0; i < L; ++i) {
            final Iterable<String> values = getColumn(records, i);
            final Column<?> column = tableHeader.headers()
                .get(i)
                .withData(typeLogic(), ColumnType.stringType(), values);
            columns.add(column);
        }
        return NewTable.of(columns);
    }

    private ColumnType<?> inferType(Iterable<String> values) {
        for (ColumnType<?> columnType : INFER_ORDER) {
            if (typeLogic().canTransform(columnType, ColumnType.stringType(), values)) {
                return columnType;
            }
        }
        return ColumnType.stringType();
    }

    private static Iterable<String> getColumn(Iterable<CSVRecord> records, int index) {
        return () -> new Iterator<String>() {
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
    }
}
