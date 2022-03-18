package io.deephaven.march;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.march.ImmutableRoundOfDetails.Builder;
import io.deephaven.time.DateTime;
import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Immutable
@MarchStyle
public abstract class RoundOfDetails {

    public static RoundOfDetails of(Table table) {
        if (table.isRefreshing()) {
            throw new IllegalArgumentException("Table must not be refreshing");
        }
        final int L = table.intSize();
        final Builder builder = ImmutableRoundOfDetails.builder();
        final ColumnSource<Integer> roundOf = table.getColumnSource("RoundOf");
        final ColumnSource<DateTime> endTimestamp = table.getColumnSource("EndTimestamp");
        for (int i = 0; i < L; ++i) {
            builder.addRounds(ImmutableRoundDetails.builder()
                    .roundOf(roundOf.getInt(i))
                    .endTime(endTimestamp.get(i).getInstant())
                    .build());
        }
        return builder.build();
    }

    public abstract List<RoundDetails> rounds();

    @Derived
    @Auxiliary
    public Map<Integer, RoundDetails> roundOfToDetails() {
        return rounds().stream().collect(Collectors.toMap(RoundDetails::roundOf, Function.identity()));
    }

    @Check
    final void checkTimestamps() {
        final Iterator<Instant> it = rounds().stream()
                .sorted(Comparator.comparing(RoundDetails::roundOf).reversed())
                .map(RoundDetails::endTime)
                .iterator();
        if (!it.hasNext()) {
            return;
        }
        Instant current = it.next();
        while (it.hasNext()) {
            Instant next = it.next();
            if (current.compareTo(next) >= 0) {
                throw new IllegalArgumentException("Rounds timestamps aren't in time-increasing order");
            }
            current = next;
        }
    }
}
