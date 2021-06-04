/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.replay.Replayer;
import io.deephaven.qst.table.time.NamedTimeProvider;
import io.deephaven.qst.table.time.OffsetTimeProvider;
import io.deephaven.qst.table.time.ScaledTimeProvider;
import io.deephaven.qst.table.time.SystemTimeProvider;
import io.deephaven.qst.table.time.TimeTable;
import java.util.Objects;

/**
 * Interface for providing the current time.
*/
public interface TimeProvider {

    static TimeProvider from(TimeTable timeTable) {
        return timeTable.timeProvider().walk(new ConvertImpl(timeTable)).getOut();
    }

    DBDateTime currentTime();

    class ConvertImpl implements io.deephaven.qst.table.time.TimeProvider.Visitor {

        private TimeTable in;
        private TimeProvider out;

        ConvertImpl(TimeTable in) {
            this.in = Objects.requireNonNull(in);
        }

        TimeProvider getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(SystemTimeProvider system) {
            out = Replayer.getTimeProvider(null); // this is what TableTools.timeTable uses
        }

        @Override
        public void visit(NamedTimeProvider named) {
            throw new IllegalArgumentException("Do not supported query-scope named time providers, yet...");
        }

        @Override
        public void visit(OffsetTimeProvider offset) {
            TimeProvider parent = offset.parent().walk(new ConvertImpl(in)).getOut();
            final long offsetNanos;
            if (offset.offset().isPresent()) {
                offsetNanos = offset.offset().get().toNanos();
            } else {
                long firstTimeNanos = in.firstTime().getEpochSecond() * 1_000_000_000L + in.firstTime().getNano();
                long current = parent.currentTime().getNanos();
                offsetNanos = firstTimeNanos - current;
            }
            out = () -> DBTimeUtils.plus(parent.currentTime(), offsetNanos);
        }

        @Override
        public void visit(ScaledTimeProvider scaled) {
            TimeProvider parent = scaled.parent().walk(new ConvertImpl(in)).getOut();
            final long firstTimeNanos = in.firstTime().getEpochSecond() * 1_000_000_000L + in.firstTime().getNano();
            out = () -> {
                long current = parent.currentTime().getNanos();
                long difference = current - firstTimeNanos;
                long newDifference = Math.round(difference * scaled.scale());
                return new DBDateTime(firstTimeNanos + newDifference);
            };
        }
    }
}
