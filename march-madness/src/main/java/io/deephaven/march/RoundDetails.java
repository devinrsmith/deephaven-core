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

    public static List<RoundDetails> of(Table table) {
        if (table.isRefreshing()) {
            throw new IllegalArgumentException("Table must not be refreshing");
        }
        final int L = table.intSize();
        final List<RoundDetails> out = new ArrayList<>(L);
        final ColumnSource<Integer> roundOf = table.getColumnSource("RoundOf");
        final ColumnSource<DateTime> endTimestamp = table.getColumnSource("EndTimestamp");
        for (int i = 0; i < L; ++i) {
            out.add(ImmutableRoundDetails.builder()
                    .roundOf(roundOf.getInt(i))
                    .endTime(endTimestamp.get(i).getInstant())
                    .build());
        }
        return out;
    }

    public abstract int roundOf();

    public abstract Instant endTime();
}
