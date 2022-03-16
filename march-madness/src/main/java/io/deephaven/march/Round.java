package io.deephaven.march;

import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Immutable
@MarchStyle
public abstract class Round {

    public abstract List<Match> matches();

    public final boolean hasTeam(int teamId) {
        return idToTeam().containsKey(teamId);
    }

    public final int matchIndex(int teamId) {
        return Objects.requireNonNull(idToIndex().get(teamId));
    }

    public final int size() {
        return matches().size();
    }

    public final int numTeams() {
        return size() * 2;
    }

    @Derived
    @Auxiliary
    public Map<Integer, Integer> idToIndex() {
        final Map<Integer, Integer> index = new HashMap<>();
        int ix = 0;
        for (Match match : matches()) {
            index.put(match.teamA().seed(), ix);
            index.put(match.teamB().seed(), ix);
            ++ix;
        }
        return Collections.unmodifiableMap(index);
    }

    @Derived
    @Auxiliary
    public Map<Integer, Team> idToTeam() {
        return Stream.concat(
                matches().stream().map(Match::teamA),
                matches().stream().map(Match::teamB))
                .collect(Collectors.toMap(Team::seed, Function.identity()));
    }

    @Check
    final void checkTeams() {
        final long distinctTeams = Stream.concat(
                matches().stream().map(Match::teamA),
                matches().stream().map(Match::teamB)).distinct().count();
        if (distinctTeams != matches().size() * 2L) {
            throw new IllegalArgumentException();
        }
    }

    @Check
    final void checkSize() {
        if (matches().isEmpty()) {
            throw new IllegalArgumentException("Can't have an empty round");
        }
    }
}
