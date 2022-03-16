package io.deephaven.march;

import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.CsvTools;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.march.ImmutableTeams.Builder;
import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
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
public abstract class Teams {

    private static final CsvSpecs SPECS = CsvSpecs.builder()
            .hasHeaderRow(true)
            .headers(Arrays.asList("Name", "Url"))
            .putParserForName("Name", Parsers.STRING)
            .putParserForName("Url", Parsers.STRING)
            .build();

    public static Teams of(Path csvPath) throws CsvReaderException, IOException {
        return ImmutableTeams.builder().table(CsvTools.readCsv(csvPath, SPECS).update("Seed=i")).build();
    }

    public abstract Table table();

    public final int size() {
        return teams().size();
    }

    public final int numRounds() {
        return Integer.numberOfTrailingZeros(size());
    }

    public final Team team(int seed) {
        return Objects.requireNonNull(seedToTeam().get(seed));
    }

    @Derived
    @Auxiliary
    public List<Team> teams() {
        final int L = table().intSize();
        final List<Team> out = new ArrayList<>(L);
        final ColumnSource<Integer> seed = table().getColumnSource("Seed", int.class);
        final ColumnSource<String> name = table().getColumnSource("Name", String.class);
        final ColumnSource<String> url = table().getColumnSource("Url", String.class);
        for (int i = 0; i < L; ++i) {
            // seed becomes id
            out.add(Team.of(seed.getInt(i), name.get(i), url.get(i)));
        }
        return out;
    }

    @Derived
    @Auxiliary
    Map<Integer, Team> seedToTeam() {
        return teams().stream().collect(Collectors.toMap(Team::seed, Function.identity()));
    }

    @Check
    final void nonRefreshing() {
        if (table().isRefreshing()) {
            throw new IllegalArgumentException("Table must be non-refreshing");
        }
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
