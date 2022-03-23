package io.deephaven.march;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.CsvTools;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;

import java.nio.file.Path;
import java.util.Collections;

public final class IpBlocklist {

    private static final CsvSpecs SPECS = CsvTools.builder()
            .hasHeaderRow(true)
            .headers(Collections.singleton("Ip"))
            .putParserForName("Ip", Parsers.STRING)
            .build();

    public static Table readCsv(Path csvPath) throws CsvReaderException {
        return CsvTools.readCsv(csvPath, SPECS);
    }
}
