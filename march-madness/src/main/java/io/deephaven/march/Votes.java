package io.deephaven.march;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.CsvTools;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedMutableTable;
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
final class Votes {

    private static final ColumnHeaders7<Instant, String, Long, String, Integer, Integer, Integer> HEADER =
            ColumnHeader.ofInstant("Timestamp")
                    .header(ColumnHeader.ofString("Ip"))
                    .header(ColumnHeader.ofLong("Session"))
                    .header(ColumnHeader.ofString("UserAgent"))
                    .header(ColumnHeader.ofInt("Bracket"))
                    .header(ColumnHeader.ofInt("MatchIndex"))
                    .header(ColumnHeader.ofInt("Team"));

    private static final CsvSpecs SPECS = CsvSpecs.builder()
            .hasHeaderRow(false)
            .headers(Arrays.asList("Timestamp", "Ip", "Session", "UserAgent", "Bracket", "MatchIndex", "Team"))
            .putParserForName("Timestamp", Parsers.DATETIME)
            .putParserForName("Ip", Parsers.STRING)
            .putParserForName("Session", Parsers.STRING)
            .putParserForName("UserAgent", Parsers.STRING)
            .putParserForName("Bracket", Parsers.INT)
            .putParserForName("MatchIndex", Parsers.INT)
            .putParserForName("Team", Parsers.INT)
            .build();

    public static Table readCsv(Path path) throws CsvReaderException {
        return CsvTools.readCsv(path, SPECS);
    }

    private final Path csvPath;
    private final MutableInputTable handler;
    private final Table readOnlyCopy;

    public Votes(Path csvPath) {
        this.csvPath = Objects.requireNonNull(csvPath);
        AppendOnlyArrayBackedMutableTable table = AppendOnlyArrayBackedMutableTable.make(TableDefinition.from(HEADER));
        handler = (MutableInputTable) table.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        readOnlyCopy = table.readOnlyCopy();
    }

    public Table getReadOnlyTable() {
        return readOnlyCopy;
    }

    public void append(Instant timestamp, String ip, long session, String userAgent, Vote vote) throws IOException {
        final Table inMemoryTable = InMemoryTable.from(HEADER.start(1)
                .row(timestamp, ip, session, userAgent, vote.bracket().id(), vote.matchIndex(), vote.team().id())
                .newTable());
        handler.add(inMemoryTable);
        writeToCsv(timestamp, ip, session, userAgent, vote);
    }

    private void writeToCsv(Instant timestamp, String ip, long session, String userAgent, Vote vote)
            throws IOException {
        final String csvLine = Stream.of(
                timestamp.toString(),
                ip,
                Long.toString(session),
                userAgent,
                Integer.toString(vote.bracket().id()),
                Integer.toString(vote.matchIndex()),
                Integer.toString(vote.team().id()))
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
