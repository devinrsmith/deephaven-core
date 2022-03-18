package io.deephaven.march;

import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

        final RoundOfDetails roundDetails = RoundOfDetails.of(component.roundsTable());

        final Optional<Round> currentRound =
                matches.init(initialTeams.toFirstRound(MarchMadnessModule.useBracketOptimalOrder()));
        if (currentRound.isPresent()) {
            new Scheduler(component.scheduler(), roundDetails, matches, potentialWinners)
                    .start(currentRound.get());
        }
    }

    private static class Scheduler {
        private final ScheduledExecutorService scheduler;
        private final RoundOfDetails allDetails;
        private final Matches matches;
        private final Table potentialWinners;

        public Scheduler(ScheduledExecutorService scheduler, RoundOfDetails allDetails, Matches matches,
                Table potentialWinners) {
            this.scheduler = Objects.requireNonNull(scheduler);
            this.allDetails = Objects.requireNonNull(allDetails);
            this.matches = Objects.requireNonNull(matches);
            this.potentialWinners = Objects.requireNonNull(potentialWinners);
        }

        public void start(Round currentRound) {
            final RoundDetails roundDetails = allDetails.roundOfToDetails().get(currentRound.roundOf());
            new Advancer(currentRound, roundDetails).scheduleInitial(Instant.now());
        }

        private class Advancer implements Callable<Void> {
            private final Round round;
            private final RoundDetails details;

            public Advancer(Round round, RoundDetails details) {
                this.round = Objects.requireNonNull(round);
                this.details = Objects.requireNonNull(details);
            }

            @Override
            public Void call() throws Exception {
                final Instant now = Instant.now();
                final Duration remaining = Duration.between(now, details.endTime());
                if (remaining.isNegative() || remaining.isZero()) {
                    if (round.isLastRound()) {
                        matches.endVoting(potentialWinners);
                        return null;
                    }
                    final Round nextRound = matches.nextRound(potentialWinners);
                    final RoundDetails roundDetails = allDetails.roundOfToDetails().get(nextRound.roundOf());
                    final Advancer advancer = new Advancer(nextRound, roundDetails);
                    // todo: don't advance the very last one
                    advancer.scheduleInitial(now);
                    return null;
                }
                scheduleNext(remaining);
                return null;
            }

            private void scheduleInitial(Instant now) {
                scheduleNext(Duration.between(now, details.endTime()));
            }

            private void scheduleNext(Duration remaining) {
                // reschedule this
                final Duration nextCheck =
                        remaining.compareTo(Duration.ofMinutes(1)) > 0 ? Duration.ofMinutes(1) : remaining;
                scheduler.schedule(this, nextCheck.toNanos(), TimeUnit.NANOSECONDS);
            }
        }
    }
}
