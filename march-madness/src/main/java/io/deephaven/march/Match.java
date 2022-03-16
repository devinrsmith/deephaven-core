package io.deephaven.march;

import org.immutables.value.Value.Immutable;

@Immutable
@MarchStyle
public abstract class Match {

    public static Match of(Team teamA, Team teamB) {
        return ImmutableMatch.builder().teamA(teamA).teamB(teamB).build();
    }

    public abstract Team teamA();

    public abstract Team teamB();
}
