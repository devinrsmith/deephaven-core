package io.deephaven.processor.appender;

import io.deephaven.qst.type.Type;

import java.math.RoundingMode;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

public abstract class InstantAppenderBase extends ObjectAppenderBase<Instant> implements InstantAppender {

    public InstantAppenderBase() {
        super(Type.instantType());
    }

    protected abstract LongAppender asEpochNanosAppender();

//    @Override
//    public final InstantAppender instantAppender() {
//        return this;
//    }

    @Override
    public final LongAppender asLongEpochAppender(TimeUnit unit) {
        return InstantUtils.asEpochLong(asEpochNanosAppender(), unit);
    }

    @Override
    public final DoubleAppender asDoubleEpochAppender(TimeUnit unit, RoundingMode roundingMode) {
        return InstantUtils.asEpochDouble(asEpochNanosAppender(), unit, roundingMode);
    }

    @Override
    public final ObjectAppender<String> asStringEpochConsumer(TimeUnit unit, RoundingMode roundingMode) {
        return InstantUtils.asEpochString(asEpochNanosAppender(), unit, roundingMode);
    }
}
