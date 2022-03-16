package io.deephaven.march;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.CsvTools;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.util.KeyedArrayBackedMutableTable;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders7;
import org.apache.commons.text.StringEscapeUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// Votes table
public final class Votes {

    private static final ColumnHeaders7<Instant, String, Long, String, Integer, Integer, Integer> HEADER =
            ColumnHeader.ofInstant("Timestamp")
                    .header(ColumnHeader.ofString("Ip"))
                    .header(ColumnHeader.ofLong("Session"))
                    .header(ColumnHeader.ofString("UserAgent"))
                    .header(ColumnHeader.ofInt("RoundOf"))
                    .header(ColumnHeader.ofInt("MatchIndex"))
                    .header(ColumnHeader.ofInt("Team"));

    private static final CsvSpecs SPECS = CsvSpecs.builder()
            .hasHeaderRow(false)
            .headers(Arrays.asList("Timestamp", "Ip", "Session", "UserAgent", "RoundOf", "MatchIndex", "Team"))
            .putParserForName("Timestamp", Parsers.DATETIME)
            .putParserForName("Ip", Parsers.STRING)
            .putParserForName("Session", Parsers.LONG)
            .putParserForName("UserAgent", Parsers.STRING)
            .putParserForName("RoundOf", Parsers.INT)
            .putParserForName("MatchIndex", Parsers.INT)
            .putParserForName("Team", Parsers.INT)
            .build();

    public static Votes of(Path csvPath) throws CsvReaderException, IOException {
        final Votes votes = new Votes(csvPath);
        if (Files.exists(csvPath)) {
            votes.handler.add(CsvTools.readCsv(csvPath, SPECS));
        }
        return votes;
    }

    private final Path csvPath;
    private final MutableInputTable handler;
    private final Table readOnlyTable;

    public Votes(Path csvPath) {
        this.csvPath = Objects.requireNonNull(csvPath);
        KeyedArrayBackedMutableTable table =
                KeyedArrayBackedMutableTable.make(TableDefinition.from(HEADER), "RoundOf", "Session", "MatchIndex");
        handler = table.mutableInputTable();
        readOnlyTable = table.readOnlyCopy();
    }

    public Table table() {
        return readOnlyTable;
    }

    public void append(Vote vote) throws IOException {
        writeToCsv(vote);
        final Table inMemoryTable = InMemoryTable.from(HEADER.start(1)
                .row(vote.timestamp(), vote.ip(), vote.session(), vote.userAgent().orElse(null), vote.roundOf(),
                        vote.matchIndex(), vote.teamId())
                .newTable());
        handler.add(inMemoryTable);
    }

    private void writeToCsv(Vote vote)
            throws IOException {
        final String csvLine = Stream.of(
                vote.timestamp().toString(),
                vote.ip(),
                Long.toString(vote.session()),
                vote.userAgent().orElse(""),
                Integer.toString(vote.roundOf()),
                Integer.toString(vote.matchIndex()),
                Integer.toString(vote.teamId()))
                .map(StringEscapeUtils::escapeCsv)
                .collect(Collectors.joining(","));
        Files.write(
                csvPath,
                Collections.singleton(csvLine),
                StandardCharsets.UTF_8,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE);
    }
}
