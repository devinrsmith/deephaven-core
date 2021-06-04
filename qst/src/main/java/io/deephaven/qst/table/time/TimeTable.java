package io.deephaven.qst.table.time;

import io.deephaven.qst.table.TableBase;
import java.time.Duration;
import java.time.Instant;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class TimeTable extends TableBase {

    public static TimeTable of(Duration tickInterval) {
        return of(tickInterval, Instant.now());
    }

    public static TimeTable of(Duration tickInterval, Instant firstTime) {
        return of(tickInterval, firstTime, TimeProvider.system());
    }

    public static TimeTable of(Duration tickInterval, Instant firstTime, TimeProvider timeProvider) {
        return ImmutableTimeTable.builder()
            .tickInterval(tickInterval)
            .firstTime(firstTime)
            .timeProvider(timeProvider)
            .build();
    }

    public abstract Duration tickInterval();

    public abstract Instant firstTime();

    public abstract TimeProvider timeProvider();
}
