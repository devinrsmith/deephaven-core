//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink;

import io.deephaven.processor.sink.Sink.StreamKey;
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
import io.deephaven.qst.type.Type.Visitor;

import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
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
    // private final List<StreamStrict> streams;
    private final Map<StreamKey, StreamStrict> streams;
    private boolean writing;

    // Note: we _could_ maintain a dirty set to only check the streams that we know have changed... given this is
    // mainly for testing and debugging, added complexity is not worth it.
    // private Set<StreamStrict> dirty;

    SinkStrict(Sink delegate) {
        this.delegate = delegate.coordinator();
        final int L = delegate.streams().size();
        this.streams = new LinkedHashMap<>(L);
        for (Entry<StreamKey, Stream> e : delegate.streams().entrySet()) {
            streams.put(e.getKey(), new StreamStrict(e.getKey(), e.getValue()));
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
        for (StreamStrict stream : streams.values()) {
            stream.checkNoneSet();
            stream.checkInSync();
        }
        delegate.sync();
        writing = false;
    }

    Sink sink() {
        return Sink.builder()
                .coordinator(this)
                .putAllStreams(streams)
                .build();
    }

    private final class StreamStrict implements Stream {
        private final StreamKey streamKey;
        private final Stream delegate;
        private final Map<Key<?>, AppenderDelegate> delegates;

        public StreamStrict(StreamKey streamKey, Stream delegate) {
            this.streamKey = streamKey;
            this.delegate = Objects.requireNonNull(delegate);
            final int L = delegate.appendersMap().size();
            final Map<Key<?>, AppenderDelegate> delegates = new LinkedHashMap<>(L);
            for (Map.Entry<Key<?>, ? extends Appender> e : delegate.appendersMap().entrySet()) {
                if (!e.getKey().type().equals(e.getValue().type())) {
                    throw new IllegalArgumentException();
                }
                delegates.put(e.getKey(), appenderDelegate(e.getValue()));
            }
            this.delegates = Map.copyOf(delegates);
        }

        void checkNoneSet() {
            for (Entry<Key<?>, AppenderDelegate> e : delegates.entrySet()) {
                if (e.getValue().isSet) {
                    throw new IllegalStateException(String.format(
                            "Appender streamKey=%s, key=%s isSet, but not advanced", streamKey, e.getKey()));
                }
            }
        }

        // todo: should we let external callers access this, for testing / logging purposes?
        long checkInSync() {
            if (delegates.isEmpty()) {
                return 0;
            }
            final Iterator<Entry<Key<?>, AppenderDelegate>> it = delegates.entrySet().iterator();
            final Entry<Key<?>, AppenderDelegate> first = it.next();
            final long pos0 = first.getValue().pos();
            while (it.hasNext()) {
                final Entry<Key<?>, AppenderDelegate> current = it.next();
                final long pos = current.getValue().pos();
                if (pos0 != pos) {
                    throw new IllegalStateException(String.format(
                            "Appenders are not in-sync for streamKey=%s, stream[%s]._pos = %d, stream[%s]._pos = %d",
                            streamKey, first.getKey(), pos0, current.getKey(), pos));
                }
            }
            return pos0;
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
            for (AppenderDelegate appender : delegates.values()) {
                if (!appender.isSet) {
                    throw new IllegalStateException("Must ensure all appenders have been set before advanceAll");
                }
            }
            checkInSync();
            for (AppenderDelegate appender : delegates.values()) {
                appender.doAdvanceInternal();
            }
            delegate.advanceAll();
        }

        @Override
        public Map<Key<?>, ? extends Appender> appendersMap() {
            return delegates;
        }

        private AppenderDelegate appenderDelegate(Appender delegate) {
            return Objects.requireNonNull(delegate.type().walk(new AppenderByType(delegate)));
        }

        private class AppenderByType implements Visitor<AppenderDelegate>, PrimitiveType.Visitor<AppenderDelegate>,
                GenericType.Visitor<AppenderDelegate> {
            private final Appender delegate;

            public AppenderByType(Appender delegate) {
                this.delegate = Objects.requireNonNull(delegate);
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
                return new ShortStrict(ShortAppender.get(delegate));
            }

            @Override
            public AppenderDelegate visit(IntType intType) {
                return new IntStrict(IntAppender.get(delegate));
            }

            @Override
            public AppenderDelegate visit(LongType longType) {
                return new LongStrict(LongAppender.get(delegate));
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
                return new ObjectImpl<>(ObjectAppender.get(delegate, boxedType));
            }

            @Override
            public AppenderDelegate visit(StringType stringType) {
                return new ObjectImpl<>(ObjectAppender.get(delegate, stringType));
            }

            @Override
            public AppenderDelegate visit(InstantType instantType) {
                return new InstantImpl(InstantAppender.get(delegate));
            }

            @Override
            public AppenderDelegate visit(ArrayType<?, ?> arrayType) {
                return new ObjectImpl<>(ObjectAppender.get(delegate, arrayType));
            }

            @Override
            public AppenderDelegate visit(CustomType<?> customType) {
                return new ObjectImpl<>(ObjectAppender.get(delegate, customType));
            }
        }

        private abstract class AppenderDelegate implements Appender {

            // final Key<?> key;
            long pos;

            boolean isSet;

            // public AppenderDelegate(Key<?> key) {
            // this.key = key;
            // }

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

            ShortStrict(ShortAppender delegate) {
                this.delegate = Objects.requireNonNull(delegate);
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

            IntStrict(IntAppender delegate) {
                this.delegate = Objects.requireNonNull(delegate);
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

            LongStrict(LongAppender delegate) {
                this.delegate = Objects.requireNonNull(delegate);
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

            ObjectImpl(ObjectAppender<T> delegate) {
                this.delegate = Objects.requireNonNull(delegate);
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

            InstantImpl(InstantAppender delegate) {
                this.delegate = Objects.requireNonNull(delegate);
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
