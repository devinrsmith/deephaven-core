package io.deephaven.march;

import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

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

        final Map<Integer, RoundDetails> allDetails = RoundDetails.of(component.roundsTable())
                .stream()
                .collect(Collectors.toMap(RoundDetails::roundOf, Function.identity()));

        final Round currentRound = matches.init(initialTeams.toFirstRound(MarchMadnessModule.useBracketOptimalOrder()));

        new Scheduler(component.scheduler(), allDetails, matches, potentialWinners)
                .start(currentRound);
    }

    private static class Scheduler {
        private final ScheduledExecutorService scheduler;
        private final Map<Integer, RoundDetails> details;
        private final Matches matches;
        private final Table potentialWinners;

        public Scheduler(ScheduledExecutorService scheduler, Map<Integer, RoundDetails> details, Matches matches, Table potentialWinners) {
            this.scheduler = Objects.requireNonNull(scheduler);
            this.details = Objects.requireNonNull(details);
            this.matches = Objects.requireNonNull(matches);
            this.potentialWinners = Objects.requireNonNull(potentialWinners);
        }

        public void start(Round currentRound) {
            final RoundDetails roundDetails = details.get(currentRound.roundOf());
            new Advancer(roundDetails.endTime()).scheduleInitial(Instant.now());
        }

        private class Advancer implements Callable<Void> {
            private final Instant endTimestamp;

            public Advancer(Instant endTimestamp) {
                this.endTimestamp = Objects.requireNonNull(endTimestamp);
            }

            @Override
            public Void call() throws Exception {
                final Instant now = Instant.now();
                final Duration remaining = Duration.between(now, endTimestamp);
                if (remaining.isNegative() || remaining.isZero()) {
                    final Round nextRound = matches.nextRound(potentialWinners);
                    final RoundDetails roundDetails = details.get(nextRound.roundOf());
                    final Advancer advancer = new Advancer(roundDetails.endTime());
                    // todo: don't advance the very last one
                    advancer.scheduleInitial(now);
                    return null;
                }
                scheduleNext(remaining);
                return null;
            }

            private void scheduleInitial(Instant now) {
                scheduleNext(Duration.between(now, endTimestamp));
            }

            private void scheduleNext(Duration remaining) {
                // reschedule this
                final Duration nextCheck = remaining.compareTo(Duration.ofMinutes(1)) > 0 ? Duration.ofMinutes(1) : remaining;
                scheduler.schedule(this, nextCheck.toNanos(), TimeUnit.NANOSECONDS);
            }
        }
    }
}
