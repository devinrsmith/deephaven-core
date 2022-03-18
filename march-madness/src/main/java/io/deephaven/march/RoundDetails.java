package io.deephaven.march;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.CsvTools;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.time.DateTime;
import org.immutables.value.Value.Immutable;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Immutable
@MarchStyle
public abstract class RoundDetails {

    private static final CsvSpecs SPECS = CsvTools.builder()
            .hasHeaderRow(true)
            .headers(Arrays.asList("RoundOf", "EndTimestamp"))
            .putParserForName("RoundOf", Parsers.INT)
            .putParserForName("EndTimestamp", Parsers.DATETIME)
            .build();

    public static Table readCsv(Path csvPath) throws CsvReaderException {
        return CsvTools.readCsv(csvPath, SPECS);
    }

    public abstract int roundOf();

    public abstract Instant endTime();
}
