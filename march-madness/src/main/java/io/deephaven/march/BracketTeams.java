package io.deephaven.march;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.CsvTools;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;

import java.nio.file.Path;
import java.util.Arrays;

final class BracketTeams {

    private static final CsvSpecs CSV_SPEC = CsvSpecs.builder()
            .hasHeaderRow(true)
            .headers(Arrays.asList("Bracket", "TeamA", "TeamB"))
            .putParserForName("Bracket", Parsers.INT)
            .putParserForName("TeamA", Parsers.INT)
            .putParserForName("TeamB", Parsers.INT)
            .build();

    public static Table readCsv(Path csvPath) throws CsvReaderException {
        return CsvTools.readCsv(csvPath, CSV_SPEC);
    }
}
