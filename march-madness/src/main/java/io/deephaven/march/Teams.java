package io.deephaven.march;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.CsvTools;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public final class Teams {

    private static final CsvSpecs SPECS = CsvSpecs.builder()
            .hasHeaderRow(true)
            .headers(Arrays.asList("Id", "Name"))
            .putParserForName("Id", Parsers.INT)
            .putParserForName("Name", Parsers.STRING)
            .build();

    public static Table of(Path csvPath) throws CsvReaderException, IOException {
        return CsvTools.readCsv(csvPath, SPECS);
    }
}
