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

    @Named("round_winners")
    Table roundWinnersTable();

    @Named("current_round")
    Table currentRoundTable();

    @Named("ip_blocklist")
    Table ipBlocklist();

    TeamDetails teamDetails();

    Matches matches();

    ScheduledExecutorService scheduler();
}
