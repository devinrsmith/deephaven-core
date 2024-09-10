//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.Logger;
import io.deephaven.processor.sink.Sink.Builder;
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

import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

final class SinkLogging implements Coordinator {
    private static final Logger log = LoggerFactory.getLogger(SinkLogging.class);

    static Sink of(String prefix, LogLevel level, List<List<Type<?>>> types) {
        return new SinkLogging(prefix, level).sink(types);
    }

    private final String prefix;
    private final LogLevel level;

    public SinkLogging(String prefix, LogLevel level) {
        this.prefix = Objects.requireNonNull(prefix);
        this.level = Objects.requireNonNull(level);
    }

    Sink sink(List<List<Type<?>>> streamTypes) {
        final Builder builder = Sink.builder().coordinator(this);
        int i = 0;
        for (List<Type<?>> appenderTypes : streamTypes) {
            builder.addStreams(streamImpl(i, appenderTypes));
            ++i;
        }
        return builder.build();
    }

    private LogEntry entry() {
        return log.getEntry(level).append(prefix);
    }

    private StreamImpl streamImpl(int ix, List<Type<?>> types) {
        return new StreamImpl(ix, types);
    }

    @Override
    public void writing() {
        entry().append(", ").append("writing").endl();
    }

    @Override
    public void sync() {
        entry().append(", ").append("sync").endl();
    }

    private final class StreamImpl implements Stream {
        private final int streamIx;
        private final List<App> appenders;

        public StreamImpl(int streamIx, List<Type<?>> types) {
            this.streamIx = streamIx;
            final int L = types.size();
            final List<App> app = new ArrayList<>(L);
            int i = 0;
            for (Type<?> type : types) {
                app.add(appender(i, type));
                ++i;
            }
            appenders = List.copyOf(app);
        }

        private LogEntry entry() {
            return SinkLogging.this.entry().append(", stream=").append(streamIx);
        }

        private App appender(int appenderIx, Type<?> type) {
            return Objects.requireNonNull(type.walk(new AppCreator(appenderIx)));
        }

        @Override
        public void ensureRemainingCapacity(long size) {
            entry().append(", ensureRemainingCapacity, size=").append(size).endl();
        }

        @Override
        public void advanceAll() {
            entry().append(", advanceAll").endl();
        }

        @Override
        public List<? extends Appender> appenders() {
            return appenders;
        }

        private class AppCreator implements Type.Visitor<App>, PrimitiveType.Visitor<App>, GenericType.Visitor<App> {
            private final int ix;

            public AppCreator(int ix) {
                this.ix = ix;
            }

            @Override
            public App visit(PrimitiveType<?> primitiveType) {
                return primitiveType.walk((PrimitiveType.Visitor<App>) this);
            }

            @Override
            public App visit(GenericType<?> genericType) {
                return genericType.walk((GenericType.Visitor<App>) this);
            }

            @Override
            public App visit(BooleanType booleanType) {
                return null;
            }

            @Override
            public App visit(ByteType byteType) {
                return null;
            }

            @Override
            public App visit(CharType charType) {
                return null;
            }

            @Override
            public App visit(ShortType shortType) {
                return new ShortLogger(ix);
            }

            @Override
            public App visit(IntType intType) {
                return new IntLogger(ix);
            }

            @Override
            public App visit(LongType longType) {
                return new LongLogger(ix);
            }

            @Override
            public App visit(FloatType floatType) {
                return null;
            }

            @Override
            public App visit(DoubleType doubleType) {
                return null;
            }

            @Override
            public App visit(BoxedType<?> boxedType) {
                return new ObjectApp<>(ix, boxedType);
            }

            @Override
            public App visit(StringType stringType) {
                return new ObjectApp<>(ix, stringType);
            }

            @Override
            public App visit(InstantType instantType) {
                return new InstantApp(ix);
            }

            @Override
            public App visit(ArrayType<?, ?> arrayType) {
                return new ObjectApp<>(ix, arrayType);
            }

            @Override
            public App visit(CustomType<?> customType) {
                return new ObjectApp<>(ix, customType);
            }
        }

        private abstract class App implements Appender {

            private final int appenderIx;

            App(int appenderIx) {
                this.appenderIx = appenderIx;
            }

            LogEntry entry() {
                return StreamImpl.this.entry()
                        .append(", appender=")
                        .append(appenderIx)
                        .append(", type=")
                        .append(type().clazz().getSimpleName());
            }
        }

        private final class ShortLogger extends App implements ShortAppender {
            ShortLogger(int appenderIx) {
                super(appenderIx);
            }

            @Override
            public void setNull() {
                entry().append(", setNull").endl();
            }

            @Override
            public void set(short value) {
                entry().append(", set, value=").append(value).endl();
            }

            @Override
            public void advance() {
                entry().append(", advance").endl();
            }
        }

        private final class IntLogger extends App implements IntAppender {
            IntLogger(int appenderIx) {
                super(appenderIx);
            }

            @Override
            public void setNull() {
                entry().append(", setNull").endl();
            }

            @Override
            public void set(int value) {
                entry().append(", set, value=").append(value).endl();
            }

            @Override
            public void advance() {
                entry().append(", advance").endl();
            }
        }

        private final class LongLogger extends App implements LongAppender {
            LongLogger(int appenderIx) {
                super(appenderIx);
            }

            @Override
            public void setNull() {
                entry().append(", setNull").endl();
            }

            @Override
            public void set(long value) {
                entry().append(", set, value=").append(value).endl();
            }

            @Override
            public void advance() {
                entry().append(", advance").endl();
            }
        }

        private final class ObjectApp<T> extends App implements ObjectAppender<T> {
            private final GenericType<T> type;

            ObjectApp(int appenderIx, GenericType<T> type) {
                super(appenderIx);
                this.type = Objects.requireNonNull(type);
            }

            @Override
            public GenericType<T> type() {
                return type;
            }

            @Override
            public void setNull() {
                entry().append(", setNull").endl();
            }

            @Override
            public void set(T value) {
                entry().append(", set, value=").append(Objects.toString(value)).endl();
            }

            @Override
            public void advance() {
                entry().append(", advance").endl();
            }
        }

        private final class InstantApp extends App implements InstantAppender {

            InstantApp(int appenderIx) {
                super(appenderIx);
            }

            @Override
            public GenericType<Instant> type() {
                return Type.instantType();
            }

            @Override
            public void setNull() {
                entry().append(", setNull").endl();
            }

            @Override
            public void set(Instant value) {
                entry().append(", set, value=").append(Objects.toString(value)).endl();
            }

            @Override
            public void advance() {
                entry().append(", advance").endl();
            }

            @Override
            public LongAppender asLongEpochAppender(TimeUnit unit) {
                return new LongAppender() {
                    @Override
                    public void setNull() {
                        entry().append(", asLongEpochAppender, setNull").endl();
                    }

                    @Override
                    public void set(long value) {
                        entry().append(", asLongEpochAppender, set, value=").append(value).endl();
                    }

                    @Override
                    public void advance() {
                        entry().append(", asLongEpochAppender, advance").endl();
                    }
                };
            }

            @Override
            public DoubleAppender asDoubleEpochAppender(TimeUnit unit, RoundingMode roundingMode) {
                return new DoubleAppender() {
                    @Override
                    public void setNull() {
                        entry().append(", asDoubleEpochAppender, setNull").endl();
                    }

                    @Override
                    public void set(double value) {
                        entry().append(", asDoubleEpochAppender, set, value=").appendDouble(value).endl();
                    }

                    @Override
                    public void advance() {
                        entry().append(", asDoubleEpochAppender, advance").endl();
                    }
                };
            }

            @Override
            public ObjectAppender<String> asStringEpochConsumer(TimeUnit unit, RoundingMode roundingMode) {
                return new ObjectAppender<>() {
                    @Override
                    public GenericType<String> type() {
                        return Type.stringType();
                    }

                    @Override
                    public void setNull() {
                        entry().append(", asStringEpochConsumer, setNull").endl();
                    }

                    @Override
                    public void set(String value) {
                        entry().append(", asStringEpochConsumer, set, value=").append(value).endl();
                    }

                    @Override
                    public void advance() {
                        entry().append(", asStringEpochConsumer, advance").endl();
                    }
                };
            }
        }
    }
}
