package io.deephaven.march;

import dagger.Module;
import dagger.Provides;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

@Module
public class MarchMadnessModule {

    public static boolean useBracketOptimalOrder() {
        return Boolean.parseBoolean(System.getProperty("deephaven.march.useBracketOptimalOrder", "true"));
    }

    public static Path dataDir() {
        return Paths.get(System.getProperty("deephaven.march.dataDir", "march-madness-data"));
    }

    private static Path votesCsv() {
        return dataDir().resolve("votes.csv");
    }

    private static Path teamCsv() {
        return Paths.get(System.getProperty("deephaven.march.teamsCsv", "teams.csv"));
    }

    @Provides
    @Singleton
    public static Votes votes(UpdateGraphProcessor ugp, Matches matches) {
        try {
            return Votes.of(ugp, matches, votesCsv());
        } catch (CsvReaderException | IOException e) {
            throw new IllegalStateException(e);
        }
    }


    @Provides
    @Singleton
    public static Matches matches(UpdateGraphProcessor ugp) {
        try {
            return Matches.of(ugp, dataDir());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Provides
    @Singleton
    public static TeamDetails teams(@Named("teams") Table teams) {
        return TeamDetails.of(teams);
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
    @Singleton
    @Named("teams")
    public static Table teamsTable() {
        try {
            return TeamDetails.readCsv(teamCsv());
        } catch (CsvReaderException e) {
            throw new IllegalStateException(e);
        }
    }


    @Provides
    @Named("matches")
    public static Table matchesTable(Matches matches) {
        return matches.table();
    }
}
