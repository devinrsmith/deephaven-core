package io.deephaven.processor.appender;

import io.deephaven.qst.type.Type;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

final class InstantUtils {

    static LongAppender asEpochLong(LongAppender epochNanosAppender, TimeUnit asTimeUnit) {
        switch (asTimeUnit) {
            case NANOSECONDS:
                return epochNanosAppender;
            case MICROSECONDS:
                return new EpochNanosLongAsMicros(epochNanosAppender);
            case MILLISECONDS:
                return new EpochNanosLongAsMillis(epochNanosAppender);
            case SECONDS:
                return new EpochNanosLongAsSeconds(epochNanosAppender);
            default:
                throw new IllegalArgumentException("Unsupported unit " + asTimeUnit);
        }
    }

    static DoubleAppender asEpochDouble(LongAppender epochNanosAppender, TimeUnit asTimeUnit, RoundingMode roundingMode) {
        switch (asTimeUnit) {
            case NANOSECONDS:
                return new EpochNanosDoubleAsNanos(epochNanosAppender, roundingMode);
            case MICROSECONDS:
                return new EpochNanosDoubleAsMicros(epochNanosAppender, roundingMode);
            case MILLISECONDS:
                return new EpochNanosDoubleAsMillis(epochNanosAppender, roundingMode);
            case SECONDS:
                return new EpochNanosDoubleAsSeconds(epochNanosAppender, roundingMode);
            default:
                throw new IllegalArgumentException("Unsupported unit " + asTimeUnit);
        }
    }

    static ObjectAppender<String> asEpochString(LongAppender epochNanosAppender, TimeUnit asTimeUnit, RoundingMode roundingMode) {
        switch (asTimeUnit) {
            case NANOSECONDS:
                return new EpochNanosStringAsNanos(epochNanosAppender, roundingMode);
            case MICROSECONDS:
                return new EpochNanosStringAsMicros(epochNanosAppender, roundingMode);
            case MILLISECONDS:
                return new EpochNanosStringAsMillis(epochNanosAppender, roundingMode);
            case SECONDS:
                return new EpochNanosStringAsSeconds(epochNanosAppender, roundingMode);
            default:
                throw new IllegalArgumentException("Unsupported unit " + asTimeUnit);
        }
    }

    static abstract class FromEpochNanosLong extends LongAppenderBase {
        final LongAppender epochNanosAppender;

        public FromEpochNanosLong(LongAppender epochNanosAppender) {
            this.epochNanosAppender = Objects.requireNonNull(epochNanosAppender);
        }

        @Override
        public final void setNull() {
            epochNanosAppender.setNull();
        }

        @Override
        public final void advance() {
            epochNanosAppender.advance();
        }
    }

    static final class EpochNanosLongAsMicros extends FromEpochNanosLong {
        public EpochNanosLongAsMicros(LongAppender epochNanosAppender) {
            super(epochNanosAppender);
        }

        @Override
        public void set(long value) {
            epochNanosAppender.set(value * 1_000);
        }
    }

    static final class EpochNanosLongAsMillis extends FromEpochNanosLong {
        public EpochNanosLongAsMillis(LongAppender epochNanosAppender) {
            super(epochNanosAppender);
        }

        @Override
        public void set(long value) {
            epochNanosAppender.set(value * 1_000_000);
        }
    }

    static final class EpochNanosLongAsSeconds extends FromEpochNanosLong {
        public EpochNanosLongAsSeconds(LongAppender epochNanosAppender) {
            super(epochNanosAppender);
        }

        @Override
        public void set(long value) {
            epochNanosAppender.set(value * 1_000_000_000);
        }
    }

    static abstract class FromEpochNanosDouble extends DoubleAppenderBase {

        final LongAppender epochNanosAppender;
        final RoundingMode roundingMode;

        public FromEpochNanosDouble(LongAppender epochNanosAppender, RoundingMode roundingMode) {
            this.epochNanosAppender = Objects.requireNonNull(epochNanosAppender);
            this.roundingMode = Objects.requireNonNull(roundingMode);
        }

        @Override
        public final void setNull() {
            epochNanosAppender.setNull();
        }

        @Override
        public final void advance() {
            epochNanosAppender.advance();
        }
    }

    static final class EpochNanosDoubleAsNanos extends FromEpochNanosDouble {
        EpochNanosDoubleAsNanos(LongAppender epochNanosAppender, RoundingMode roundingMode) {
            super(epochNanosAppender, roundingMode);
        }

        @Override
        public void set(double value) {
            epochNanosAppender.set(scaleAndRound(value, 0, roundingMode));
        }
    }

    static final class EpochNanosDoubleAsMicros extends FromEpochNanosDouble {
        EpochNanosDoubleAsMicros(LongAppender epochNanosAppender, RoundingMode roundingMode) {
            super(epochNanosAppender, roundingMode);
        }

        @Override
        public void set(double value) {
            epochNanosAppender.set(scaleAndRound(value, 3, roundingMode));
        }
    }

    static final class EpochNanosDoubleAsMillis extends FromEpochNanosDouble {
        EpochNanosDoubleAsMillis(LongAppender epochNanosAppender, RoundingMode roundingMode) {
            super(epochNanosAppender, roundingMode);
        }

        @Override
        public void set(double value) {
            epochNanosAppender.set(scaleAndRound(value, 6, roundingMode));
        }
    }

    static final class EpochNanosDoubleAsSeconds extends FromEpochNanosDouble {
        EpochNanosDoubleAsSeconds(LongAppender epochNanosAppender, RoundingMode roundingMode) {
            super(epochNanosAppender, roundingMode);
        }

        @Override
        public void set(double value) {
            epochNanosAppender.set(scaleAndRound(value, 9, roundingMode));
        }
    }

    private static long scaleAndRound(double val, int scale, RoundingMode roundingMode) {
        // Note: this is different from BigDecimal.valueOf(val)... todo
        return new BigDecimal(val).scaleByPowerOfTen(scale).setScale(0, roundingMode).longValueExact();
    }

    static abstract class FromEpochNanosString extends ObjectAppenderBase<String> {

        final LongAppender epochNanosAppender;
        final RoundingMode roundingMode;

        public FromEpochNanosString(LongAppender epochNanosAppender, RoundingMode roundingMode) {
            super(Type.stringType());
            this.epochNanosAppender = Objects.requireNonNull(epochNanosAppender);
            this.roundingMode = Objects.requireNonNull(roundingMode);
        }

        @Override
        public final void setNull() {
            epochNanosAppender.setNull();
        }

        @Override
        public final void advance() {
            epochNanosAppender.advance();
        }
    }

    static final class EpochNanosStringAsNanos extends FromEpochNanosString {
        EpochNanosStringAsNanos(LongAppender epochNanosAppender, RoundingMode roundingMode) {
            super(epochNanosAppender, roundingMode);
        }

        @Override
        public void set(String value) {
            epochNanosAppender.set(scaleAndRound(value, 0, roundingMode));
        }
    }

    static final class EpochNanosStringAsMicros extends FromEpochNanosString {
        EpochNanosStringAsMicros(LongAppender epochNanosAppender, RoundingMode roundingMode) {
            super(epochNanosAppender, roundingMode);
        }

        @Override
        public void set(String value) {
            epochNanosAppender.set(scaleAndRound(value, 3, roundingMode));
        }
    }

    static final class EpochNanosStringAsMillis extends FromEpochNanosString {
        EpochNanosStringAsMillis(LongAppender epochNanosAppender, RoundingMode roundingMode) {
            super(epochNanosAppender, roundingMode);
        }

        @Override
        public void set(String value) {
            epochNanosAppender.set(scaleAndRound(value, 6, roundingMode));
        }
    }

    static final class EpochNanosStringAsSeconds extends FromEpochNanosString {
        EpochNanosStringAsSeconds(LongAppender epochNanosAppender, RoundingMode roundingMode) {
            super(epochNanosAppender, roundingMode);
        }

        @Override
        public void set(String value) {
            epochNanosAppender.set(scaleAndRound(value, 9, roundingMode));
        }
    }

    private static long scaleAndRound(String val, int scale, RoundingMode roundingMode) {
        return new BigDecimal(val).scaleByPowerOfTen(scale).setScale(0, roundingMode).longValueExact();
    }
}
