package io.deephaven.march;

import dagger.Module;
import dagger.Provides;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

@Module
public class MarchMadnessModule {

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
        return TableTools.emptyTable(1);
    }

    @Provides
    @Named("matches")
    public static Table matchesTable(Matches matches) {
        return matches.table();
    }
}
