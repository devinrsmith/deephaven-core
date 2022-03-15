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
import java.util.stream.Stream;

@Immutable
@MarchStyle
public abstract class Bracket {

    public abstract int id();

    public abstract List<Match> matches();

    public boolean hasTeam(Team team) {
        return teamMatchIndex().containsKey(team);
    }

    public int matchIndex(Team team) {
        return Objects.requireNonNull(teamMatchIndex().get(team));
    }

    @Derived
    @Auxiliary
    public Map<Team, Integer> teamMatchIndex() {
        final Map<Team, Integer> index = new HashMap<>();
        int ix = 0;
        for (Match match : matches()) {
            index.put(match.teamA(), ix);
            index.put(match.teamB(), ix);
            ++ix;
        }
        return Collections.unmodifiableMap(index);
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
}
