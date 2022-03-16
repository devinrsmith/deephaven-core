package io.deephaven.march;

import org.immutables.value.Value.Immutable;

import java.util.Set;

@Immutable
@MarchStyle
public abstract class Match {

    public static Match of(Team teamA, Team teamB) {
        return ImmutableMatch.builder().teamA(teamA).teamB(teamB).build();
    }

    public abstract Team teamA();

    public abstract Team teamB();

    public final Team getWinner(Set<Integer> winners) {
        if (winners.contains(teamA().seed())) {
            if (winners.contains(teamB().seed())) {
                throw new IllegalStateException("Can't have two winners");
            }
            return teamA();
        }
        if (winners.contains(teamB().seed())) {
            return teamB();
        }
        throw new IllegalStateException("Must have one winner");
    }
}
