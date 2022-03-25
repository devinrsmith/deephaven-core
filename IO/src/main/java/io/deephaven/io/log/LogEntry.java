/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log;

import io.deephaven.base.MathUtil;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.text.TimestampBuffer;
import io.deephaven.base.text.TimestampBufferMicros;

import java.nio.ByteBuffer;

public interface LogEntry extends LogOutput, LogSink.Element {

    // initializes this entry, prepares it to be written to the given sink on end() or endl()
    LogEntry start(LogSink sink, LogLevel level);

    LogEntry start(LogSink sink, LogLevel level, long currentTimeMicros);

    LogEntry start(LogSink sink, LogLevel level, long currentTimeMicros, Throwable t);

    LogEntry end();

    LogEntry endl();

    LogEntry append(boolean b);

    LogEntry append(char c);

    LogEntry append(short s);

    LogEntry append(int i);

    LogEntry append(long l);

    LogEntry appendDouble(double f);

    LogEntry append(LogOutputAppendable appendable);

    LogEntry append(LongFormatter formatter, long n);

    <T> LogEntry append(ObjFormatter<T> objFormatter, T t);

    <T> LogEntry append(ObjIntIntFormatter<T> objFormatter, T t, int nOffset, int nLength);

    <T, U> LogEntry append(ObjObjFormatter<T, U> objObjFormatter, T t, U u);

    LogEntry append(CharSequence seq);

    LogEntry append(CharSequence seq, int start, int length);

    LogEntry append(ByteBuffer buffer);

    LogEntry appendTimestamp(long utcMillis, TimestampBuffer tb);

    LogEntry appendTimestampMicros(long utcMicros, TimestampBufferMicros tb);

    LogEntry append(Throwable t);

    LogEntry append(byte[] ba);

    LogEntry append(byte[] ba, int pos, int length);

    LogEntry append(byte[] ba, byte terminator);

    // appenders for boxed types - have to handle nulls
    default LogEntry append(Boolean b) {
        return b == null ? append("null") : append((boolean) b);
    }

    default LogEntry append(Character c) {
        return c == null ? append("null") : append((char) c);
    }

    default LogEntry append(Short s) {
        return s == null ? append("null") : append((short) s);
    }

    default LogEntry append(Integer i) {
        return i == null ? append("null") : append((int) i);
    };

    default LogEntry append(Long l) {
        return l == null ? append("null") : append((long) l);
    };

    default LogEntry appendDouble(Double f) {
        return f == null ? append("null") : appendDouble((double) f);
    }

    /**
     * Append a double to the given number of decimal places, rounding up.
     *
     * @param f a double value to append to the logEntry
     * @param decimalPlaces a positive integer between 0 and 9
     * @return the resulting {@code LogEntry}
     */
    default LogEntry appendPositiveDouble(double f, int decimalPlaces) {
        final int decimalPlacesAsPowerOf10 = MathUtil.pow10(decimalPlaces);
        final long lf = (long) (0.5 + f * decimalPlacesAsPowerOf10);
        final long integral = lf / decimalPlacesAsPowerOf10;
        LogEntry r = append(integral);
        if (decimalPlaces == 0) {
            return r;
        }
        final int fractional = (int) (lf % decimalPlacesAsPowerOf10);
        r = r.append(".");
        final int base10FractionalDigits = MathUtil.base10digits(fractional);
        final int leadingZeroes = decimalPlaces - base10FractionalDigits;
        switch (leadingZeroes) {
            case 9:
                r = r.append("000000000");
            case 8:
                r = r.append("00000000");
                break;
            case 7:
                r = r.append("0000000");
                break;
            case 6:
                r = r.append("000000");
                break;
            case 5:
                r = r.append("00000");
                break;
            case 4:
                r = r.append("0000");
                break;
            case 3:
                r = r.append("000");
                break;
            case 2:
                r = r.append("00");
                break;
            case 1:
                r = r.append("0");
                break;
        }
        if (fractional == 0) {
            return r;
        }
        return r.append(fractional);
    }

    LogEntry nf();

    LogEntry nl();


    // ---------------------------------------------------------------------------------------------
    // null implementation
    // ---------------------------------------------------------------------------------------------

    Null NULL = new Null();

    class Null extends LogOutput.Null implements LogEntry {
        @Override
        public LogEntry start(LogSink sink, LogLevel level) {
            return this;
        }

        @Override
        public LogEntry start(LogSink sink, LogLevel level, long currentTimeMicros) {
            return this;
        }

        @Override
        public LogEntry start(LogSink sink, LogLevel level, long currentTimeMicros, Throwable t) {
            return this;
        }

        @Override
        public LogEntry end() {
            return this;
        }

        @Override
        public LogEntry endl() {
            return this;
        }

        @Override
        public LogEntry append(final boolean b) {
            return this;
        }

        @Override
        public LogEntry append(final char c) {
            return this;
        }

        @Override
        public LogEntry append(final short s) {
            return this;
        }

        @Override
        public LogEntry append(final int i) {
            return this;
        }

        @Override
        public LogEntry append(final long l) {
            return this;
        }

        @Override
        public LogEntry appendDouble(final double f) {
            return this;
        }

        @Override
        public LogEntry append(final LogOutputAppendable appendable) {
            return this;
        }

        @Override
        public LogEntry append(LongFormatter formatter, long n) {
            return this;
        }

        @Override
        public <T> LogEntry append(ObjFormatter<T> objFormatter, T t) {
            return this;
        }

        @Override
        public <T> LogEntry append(ObjIntIntFormatter<T> objFormatter, T t, int nOffset, int nLength) {
            return this;
        }

        @Override
        public <T, U> LogEntry append(ObjObjFormatter<T, U> objObjFormatter, T t, U u) {
            return this;
        }

        @Override
        public LogEntry append(final CharSequence seq) {
            return this;
        }

        @Override
        public LogEntry append(final CharSequence seq, final int start, final int length) {
            return this;
        }

        @Override
        public LogEntry append(final ByteBuffer buffer) {
            return this;
        }

        @Override
        public LogEntry appendTimestamp(final long utcMillis, final TimestampBuffer tb) {
            return this;
        }

        @Override
        public LogEntry appendTimestampMicros(final long utcMicros, final TimestampBufferMicros tb) {
            return this;
        }

        @Override
        public LogEntry append(Throwable t) {
            return this;
        }

        @Override
        public LogEntry append(byte[] ba) {
            return this;
        }

        @Override
        public LogEntry append(byte[] ba, int pos, int length) {
            return this;
        }

        @Override
        public LogEntry append(byte[] ba, byte terminator) {
            return this;
        }

        @Override
        public LogEntry nf() {
            return this;
        }

        @Override
        public LogEntry nl() {
            return this;
        }

        // from LogSink.Element
        @Override
        public long getTimestampMicros() {
            return 0;
        }

        @Override
        public LogLevel getLevel() {
            return LogLevel.INFO;
        }

        @Override
        public Throwable getThrowable() {
            return null;
        }

        @Override
        public LogOutput writing(LogOutput outputBuffer) {
            return outputBuffer;
        }

        @Override
        public void written(LogOutput outputBuffer) {}
    }
}
