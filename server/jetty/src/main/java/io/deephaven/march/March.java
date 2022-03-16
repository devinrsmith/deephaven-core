package io.deephaven.march;

import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;

public class March {

    private static MarchComponent component;

    public static synchronized void init(MarchComponent marchComponent) {
        if (component != null) {
            throw new IllegalStateException();
        }
        component = marchComponent;
    }

    public static MarchComponent get() {
        return component;
    }

    public static void start(Table potentialWinners) throws IOException, CsvReaderException {

        try {
            Files.createDirectory(MarchMadnessModule.dataDir());
        } catch (FileAlreadyExistsException e) {
            // ignore
        }

        final MarchComponent component = get();
        final Matches matches = component.matches();
        final TeamDetails initialTeams = component.teamDetails();
        matches.init(initialTeams.toRound());
    }
}
