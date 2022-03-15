package io.deephaven.march;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

@Immutable
@MarchStyle
public abstract class Vote {
    public abstract Bracket bracket();

    public abstract Team team();

    public int matchIndex() {
        return bracket().matchIndex(team());
    }

    @Check
    final void checkTeam() {
        if (!bracket().hasTeam(team())) {
            throw new IllegalArgumentException();
        }
    }
}
