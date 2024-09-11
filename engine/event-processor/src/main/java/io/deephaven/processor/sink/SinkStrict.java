//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink;

import io.deephaven.processor.sink.appender.Appender;
import io.deephaven.processor.sink.appender.DoubleAppender;
import io.deephaven.processor.sink.appender.InstantAppender;
import io.deephaven.processor.sink.appender.IntAppender;
import io.deephaven.processor.sink.appender.LongAppender;
import io.deephaven.processor.sink.appender.ObjectAppender;
import io.deephaven.processor.sink.appender.ShortAppender;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import io.deephaven.qst.type.Type.Visitor;

import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

final class SinkStrict implements Coordinator {

    static Sink of(Sink sink) {
        return new SinkStrict(sink).sink();
    }

    static OptionalLong pos(Stream stream) {
        return stream instanceof StreamStrict ? OptionalLong.of(((StreamStrict) stream).checkInSync())
                : OptionalLong.empty();
    }

    static long pos(Appender appender) {
        if (appender instanceof StreamStrict.AppenderDelegate) {
            return ((StreamStrict.AppenderDelegate) appender).pos();
        }
        return -1;
    }

    private final Coordinator delegate;
    private final List<StreamStrict> streams;
    private boolean writing;

    // Note: we _could_ maintain a dirty set to only check the streams that we know have changed... given this is
    // mainly for testing and debugging, added complexity is not worth it.
    // private Set<StreamStrict> dirty;

    SinkStrict(Sink delegate) {
        this.delegate = delegate.coordinator();
        final int L = delegate.streams().size();
        this.streams = new ArrayList<>(L);
        for (int i = 0; i < L; i++) {
            streams.add(new StreamStrict(i, delegate.streams().get(i)));
        }
    }

    @Override
    public void writing() {
        if (writing) {
            throw new IllegalStateException("Already in writing");
        }
        writing = true;
        delegate.writing();
    }

    @Override
    public void sync() {
        if (!writing) {
            throw new IllegalStateException("Can only sync if writing");
        }
        for (StreamStrict stream : streams) {
            stream.checkNoneSet();
            stream.checkInSync();
        }
        delegate.sync();
        writing = false;
    }

    Sink sink() {
        return Sink.builder()
                .coordinator(this)
                .addAllStreams(streams)
                .build();
    }

    private final class StreamStrict implements Stream {
        private final int streamIx;
        private final Stream delegate;
        private final List<AppenderDelegate> delegates;

        public StreamStrict(int streamIx, Stream delegate) {
            this.streamIx = streamIx;
            this.delegate = Objects.requireNonNull(delegate);
            final int L = delegate.appenders().size();
            final List<AppenderDelegate> delegates = new ArrayList<>(L);
            for (int i = 0; i < L; i++) {
                delegates.add(appenderDelegate(delegate.appenders().get(i).type(), i));
            }
            this.delegates = List.copyOf(delegates);
        }

        void checkNoneSet() {
            final int L = delegates.size();
            for (int i = 0; i < L; i++) {
                if (delegates.get(i).isSet) {
                    throw new IllegalStateException(String.format(
                            "Appender streamIx=%d, stream.appenders().get(%d) isSet, but not advanced", streamIx, 0));
                }
            }
        }

        // todo: should we let external callers access this, for testing / logging purposes?
        long checkInSync() {
            final int L = delegates.size();
            if (L == 0) {
                return 0;
            }
            final long pos0 = delegates.get(0).pos();
            for (int i = 1; i < L; ++i) {
                final long pos = delegates.get(i).pos();
                if (pos0 != pos) {
                    throw new IllegalStateException(String.format(
                            "Appenders are not in-sync for streamIx=%d, stream.appenders().get(0)._pos = %d, stream.appenders().get(%d)._pos = %d",
                            streamIx, pos0, i, pos));
                }
            }
            return pos0;
        }

        Appender inner(int ix) {
            return delegate.appenders().get(ix);
        }

        @Override
        public void ensureRemainingCapacity(long size) {
            if (!writing) {
                throw new IllegalStateException("Must be in writing mode to call this");
            }
            // todo: should we allow 0?
            if (size < 0) {
                throw new IllegalArgumentException();
            }
            delegate.ensureRemainingCapacity(size);
        }

        @Override
        public void advanceAll() {
            if (!writing) {
                throw new IllegalStateException("Must be in writing mode to call this");
            }
            for (AppenderDelegate appender : delegates) {
                if (!appender.isSet) {
                    throw new IllegalStateException("Must ensure all appenders have been set before advanceAll");
                }
            }
            checkInSync();
            for (AppenderDelegate appender : delegates) {
                appender.doAdvanceInternal();
            }
            delegate.advanceAll();
        }

        @Override
        public List<? extends Appender> appenders() {
            return delegates;
        }

        private AppenderDelegate appenderDelegate(Type<?> type, int ix) {
            return Objects.requireNonNull(type.walk(new AppenderByType(ix)));
        }

        private class AppenderByType implements Visitor<AppenderDelegate>, PrimitiveType.Visitor<AppenderDelegate>,
                GenericType.Visitor<AppenderDelegate> {
            private final int ix;

            public AppenderByType(int ix) {
                this.ix = ix;
            }

            @Override
            public AppenderDelegate visit(PrimitiveType<?> primitiveType) {
                return primitiveType.walk((PrimitiveType.Visitor<AppenderDelegate>) this);
            }

            @Override
            public AppenderDelegate visit(GenericType<?> genericType) {
                return genericType.walk((GenericType.Visitor<AppenderDelegate>) this);
            }

            @Override
            public AppenderDelegate visit(BooleanType booleanType) {
                return null;
            }

            @Override
            public AppenderDelegate visit(ByteType byteType) {
                return null;
            }

            @Override
            public AppenderDelegate visit(CharType charType) {
                return null;
            }

            @Override
            public AppenderDelegate visit(ShortType shortType) {
                return new ShortStrict(ix);
            }

            @Override
            public AppenderDelegate visit(IntType intType) {
                return new IntStrict(ix);
            }

            @Override
            public AppenderDelegate visit(LongType longType) {
                return new LongStrict(ix);
            }

            @Override
            public AppenderDelegate visit(FloatType floatType) {
                return null;
            }

            @Override
            public AppenderDelegate visit(DoubleType doubleType) {
                return null;
            }

            @Override
            public AppenderDelegate visit(BoxedType<?> boxedType) {
                return new ObjectImpl<>(boxedType, ix);
            }

            @Override
            public AppenderDelegate visit(StringType stringType) {
                return new ObjectImpl<>(stringType, ix);
            }

            @Override
            public AppenderDelegate visit(InstantType instantType) {
                return new InstantImpl(ix);
            }

            @Override
            public AppenderDelegate visit(ArrayType<?, ?> arrayType) {
                return new ObjectImpl<>(arrayType, ix);
            }

            @Override
            public AppenderDelegate visit(CustomType<?> customType) {
                return new ObjectImpl<>(customType, ix);
            }
        }

        private abstract class AppenderDelegate implements Appender {

            final int ix;
            long pos;

            boolean isSet;

            public AppenderDelegate(int ix) {
                this.ix = ix;
            }

            long pos() {
                return pos;
            }

            void doSet() {
                if (!writing) {
                    throw new IllegalStateException("Must not set if not writing");
                }
                isSet = true;
                // setting multiple times is "ok".
                // todo: should we have a setting that disallows this?
            }

            void doAdvance() {
                if (!writing) {
                    throw new IllegalStateException("Must not advance if not writing");
                }
                // if (spec.isRowOriented()) {
                // // todo: should this be a limitation
                // throw new IllegalStateException("Can't advance if row-oriented, use advanceAll");
                // }
                if (!isSet) {
                    throw new IllegalStateException("Can't advance without setting");
                }
                doAdvanceInternal();
            }

            void doAdvanceInternal() {
                isSet = false;
                ++pos;
            }
        }

        private final class ShortStrict extends AppenderDelegate implements ShortAppender {
            private final ShortAppender delegate;

            ShortStrict(int ix) {
                super(ix);
                this.delegate = ShortAppender.get(inner(ix));
            }

            @Override
            public void setNull() {
                doSet();
                delegate.setNull();
            }

            @Override
            public void set(short value) {
                doSet();
                delegate.set(value);
            }

            @Override
            public void advance() {
                doAdvance();
                delegate.advance();
            }
        }

        private final class IntStrict extends AppenderDelegate implements IntAppender {
            private final IntAppender delegate;

            IntStrict(int ix) {
                super(ix);
                this.delegate = IntAppender.get(inner(ix));
            }

            @Override
            public void setNull() {
                doSet();
                delegate.setNull();
            }

            @Override
            public void set(int value) {
                doSet();
                delegate.set(value);
            }

            @Override
            public void advance() {
                doAdvance();
                delegate.advance();
            }
        }

        private final class LongStrict extends AppenderDelegate implements LongAppender {
            private final LongAppender delegate;

            LongStrict(int ix) {
                super(ix);
                this.delegate = LongAppender.get(inner(ix));
            }

            @Override
            public void setNull() {
                doSet();
                delegate.setNull();
            }

            @Override
            public void set(long value) {
                doSet();
                delegate.set(value);
            }

            @Override
            public void advance() {
                doAdvance();
                delegate.advance();
            }
        }

        private final class ObjectImpl<T> extends AppenderDelegate implements ObjectAppender<T> {
            private final ObjectAppender<T> delegate;

            ObjectImpl(GenericType<T> genericType, int ix) {
                super(ix);
                this.delegate = ObjectAppender.get(inner(ix), genericType);
            }

            @Override
            public GenericType<T> type() {
                return delegate.type();
            }

            @Override
            public void setNull() {
                doSet();
                delegate.setNull();
            }

            @Override
            public void set(T value) {
                doSet();
                delegate.set(value);
            }

            @Override
            public void advance() {
                doAdvance();
                delegate.advance();
            }
        }

        private final class InstantImpl extends AppenderDelegate implements InstantAppender {
            private final InstantAppender delegate;

            InstantImpl(int ix) {
                super(ix);
                this.delegate = InstantAppender.get(inner(ix));
            }

            @Override
            public GenericType<Instant> type() {
                return delegate.type();
            }

            @Override
            public void setNull() {
                doSet();
                delegate.setNull();
            }

            @Override
            public void set(Instant value) {
                doSet();
                delegate.set(value);
            }

            @Override
            public void advance() {
                doAdvance();
                delegate.advance();
            }

            @Override
            public LongAppender asLongEpochAppender(TimeUnit unit) {
                final LongAppender delegateImpl = delegate.asLongEpochAppender(unit);
                return new LongAppender() {
                    @Override
                    public void setNull() {
                        doSet();
                        delegateImpl.setNull();
                    }

                    @Override
                    public void set(long value) {
                        doSet();
                        delegateImpl.set(value);
                    }

                    @Override
                    public void advance() {
                        doAdvance();
                        delegateImpl.advance();
                    }
                };
            }

            @Override
            public DoubleAppender asDoubleEpochAppender(TimeUnit unit, RoundingMode roundingMode) {
                final DoubleAppender delegateImpl = delegate.asDoubleEpochAppender(unit, roundingMode);
                return new DoubleAppender() {
                    @Override
                    public void setNull() {
                        doSet();
                        delegateImpl.setNull();
                    }

                    @Override
                    public void set(double value) {
                        doSet();
                        delegateImpl.set(value);
                    }

                    @Override
                    public void advance() {
                        doAdvance();
                        delegateImpl.advance();
                    }
                };
            }

            @Override
            public ObjectAppender<String> asStringEpochConsumer(TimeUnit unit, RoundingMode roundingMode) {
                final ObjectAppender<String> delegateImpl = delegate.asStringEpochConsumer(unit, roundingMode);
                return new ObjectAppender<>() {
                    @Override
                    public GenericType<String> type() {
                        return delegateImpl.type();
                    }

                    @Override
                    public void setNull() {
                        doSet();
                        delegateImpl.setNull();
                    }

                    @Override
                    public void set(String value) {
                        doSet();
                        delegateImpl.set(value);
                    }

                    @Override
                    public void advance() {
                        doAdvance();
                        delegateImpl.advance();
                    }
                };
            }
        }
    }
}
