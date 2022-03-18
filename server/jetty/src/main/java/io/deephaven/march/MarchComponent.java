package io.deephaven.march;

import io.deephaven.engine.table.Table;

import javax.inject.Named;
import java.util.concurrent.ScheduledExecutorService;

public interface MarchComponent {

    @Named("teams")
    Table teamsTable();

    @Named("matches")
    Table matchesTable();

    @Named("votes")
    Table votesTable();

    @Named("rounds")
    Table roundsTable();

    TeamDetails teamDetails();

    Matches matches();

    ScheduledExecutorService scheduler();
}
