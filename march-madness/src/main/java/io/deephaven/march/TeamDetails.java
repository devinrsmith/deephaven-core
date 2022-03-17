package io.deephaven.march;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.CsvTools;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.march.ImmutableRound.Builder;
import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@Immutable
@MarchStyle
public abstract class TeamDetails {

    private static final CsvSpecs SPECS = CsvSpecs.builder()
            .hasHeaderRow(true)
            .headers(Arrays.asList("Seed", "Name", "Url"))
            .putParserForName("Seed", Parsers.INT)
            .putParserForName("Name", Parsers.STRING)
            .putParserForName("Url", Parsers.STRING)
            .build();

    private static List<Team> teams(Table table) {
        if (table.isRefreshing()) {
            throw new IllegalArgumentException("Table must not be refreshing");
        }
        final int L = table.intSize();
        final List<Team> out = new ArrayList<>(L);
        final ColumnSource<Integer> seed = table.getColumnSource("Seed", int.class);
        final ColumnSource<String> name = table.getColumnSource("Name", String.class);
        final ColumnSource<String> url = table.getColumnSource("Url", String.class);
        for (int i = 0; i < L; ++i) {
            out.add(Team.of(seed.getInt(i), name.get(i), url.get(i)));
        }
        return out;
    }

    public static Table readCsv(Path csvPath) throws CsvReaderException {
        return CsvTools.readCsv(csvPath, SPECS);
    }

    public static TeamDetails of(Table table) {
        return of(teams(table));
    }

    public static TeamDetails of(List<Team> teams) {
        return ImmutableTeamDetails.builder().addAllTeams(teams).build();
    }

    public abstract List<Team> teams();

    public final int size() {
        return teams().size();
    }

    public final int numRounds() {
        return Integer.numberOfTrailingZeros(size());
    }

    public final Team team(int seed) {
        return Objects.requireNonNull(seedToTeam().get(seed));
    }

    public final Round toFirstRound(boolean useBracketOptimalOrder) {
        final Builder builder = ImmutableRound.builder();
        final List<Team> teams;
        if (useBracketOptimalOrder) {
            teams = BracketOrdering.bracketOptimalOrder(teams()).collect(Collectors.toList());
        } else {
            teams = teams();
        }
        final int L = teams.size();
        for (int i = 0; i < L; i += 2) {
            final Team teamA = teams.get(i);
            final Team teamB = teams.get(i + 1);
            builder.addMatches(Match.of(teamA, teamB));
        }
        return builder.build();
    }

    @Derived
    @Auxiliary
    Map<Integer, Team> seedToTeam() {
        return teams().stream().collect(Collectors.toMap(Team::seed, Function.identity()));
    }

    @Check
    final void checkSize() {
        if (size() < 2) {
            throw new IllegalArgumentException("Must have at least 2 teams");
        }
        if ((size() & (size() - 1)) != 0) {
            throw new IllegalArgumentException("Must have the number of teams be a power of 2");
        }
    }
}
