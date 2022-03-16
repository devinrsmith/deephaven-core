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

    public static Path dataDir() {
        return Paths.get(System.getProperty("deephaven.march.dataDir", "march-madness-data"));
    }

    @Provides
    @Singleton
    public static Votes votes() {
        try {
            return Votes.of(dataDir().resolve("votes.csv"));
        } catch (CsvReaderException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Provides
    @Singleton
    public static Matches matches() {
        try {
            return Matches.of(dataDir());
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
        final Path teamsCsv = Paths.get(System.getProperty("deephaven.march.teamsCsv", "teams.csv"));
        try {
            return TeamDetails.readCsv(teamsCsv);
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
