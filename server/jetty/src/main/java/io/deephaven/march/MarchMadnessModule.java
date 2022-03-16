package io.deephaven.march;

import dagger.Module;
import dagger.Provides;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

@Module
public class MarchMadnessModule {

    // @Provides
    // public static Round bracket() {
    // final int bracketId = 2; // increment this as appropriate
    // final Team team1 = ImmutableTeam.builder().id(1).name("Team 1").url("1").build();
    //// final Team team2 = ImmutableTeam.builder().id(2).name("Team 2").url("2").build();
    //// final Team team3 = ImmutableTeam.builder().id(3).name("Team 3").url("3").build();
    // final Team team4 = ImmutableTeam.builder().id(4).name("Team 4").url("4").build();
    // return ImmutableRound.builder()
    // .id(bracketId)
    // .addMatches(
    // Match.of(team1, team4))
    // .build();
    // }

    @Provides
    @Singleton
    public static Votes votes() {
        final Path votesCsv = Paths.get(System.getProperty("deephaven.march.votesCsv", "votes.csv"));
        try {
            return Votes.of(votesCsv);
        } catch (CsvReaderException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Provides
    @Singleton
    public static Matches matches(Teams teams) {
        try {
            return Matches.of(teams);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Provides
    @Singleton
    public static Teams teams() {
        final Path teamsCsv = Paths.get(System.getProperty("deephaven.march.teamsCsv", "teams.csv"));
        try {
            return Teams.of(teamsCsv);
        } catch (CsvReaderException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Provides
    @Named("votes")
    public static Table votesTable(Votes votes) {
        return votes.table();
    }

    @Provides
    @Singleton
    @Named("votes_count")
    public static Table votesCountTable(@Named("votes") Table votes) {
        return votes.countBy("Count", "Round", "MatchIndex", "Team");
    }

    @Provides
    @Named("teams")
    public static Table teamsTable(Teams teams) {
        return teams.table();
    }

    @Provides
    @Named("matches")
    public static Table matchesTable(Matches matches) {
        return matches.table();
    }

    // @Provides
    // @Singleton
    // @Named("rounds")
    // public static Table roundsTable() {
    // final Path teamsCsv = Paths.get(System.getProperty("deephaven.march.roundsCsv", "rounds.csv"));
    // try {
    // return Rounds.of(teamsCsv);
    // } catch (CsvReaderException | IOException e) {
    // throw new IllegalStateException(e);
    // }
    // }

}
