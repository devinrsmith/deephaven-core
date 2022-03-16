package io.deephaven.march;

import io.deephaven.engine.table.Table;

import javax.inject.Named;

public interface MarchComponent {

    @Named("teams")
    Table teamsTable();

    @Named("matches")
    Table matchesTable();

    @Named("votes")
    Table votesTable();

    TeamDetails teamDetails();

    Matches matches();
}
