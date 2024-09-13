//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.Logger;
import io.deephaven.processor.sink.Sink.Builder;
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
import io.deephaven.qst.type.Type;

import java.math.RoundingMode;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

final class SinkLogging implements Coordinator {
    private static final Logger log = LoggerFactory.getLogger(SinkLogging.class);

    static Sink of(String prefix, LogLevel level, Map<StreamKey, Keys> streamTypes) {
        return new SinkLogging(prefix, level).sink(streamTypes);
    }

    private final String prefix;
    private final LogLevel level;

    public SinkLogging(String prefix, LogLevel level) {
        this.prefix = Objects.requireNonNull(prefix);
        this.level = Objects.requireNonNull(level);
    }

    Sink sink(Map<StreamKey, Keys> streamTypes) {
        final Builder builder = Sink.builder().coordinator(this);
        for (Entry<StreamKey, Keys> e : streamTypes.entrySet()) {
            builder.putStreams(e.getKey(), streamImpl(e.getKey(), e.getValue()));
        }
        return builder.build();
    }

    private LogEntry entry() {
        return log.getEntry(level).append(prefix);
    }

    private StreamImpl streamImpl(StreamKey streamKey, Keys keys) {
        return new StreamImpl(streamKey, keys);
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
        private final StreamKey streamKey;
        private final Map<Key<?>, App> appenders;

        public StreamImpl(StreamKey streamKey, Keys keys) {
            this.streamKey = Objects.requireNonNull(streamKey);
            final int L = keys.keys().size();
            final Map<Key<?>, App> app = new LinkedHashMap<>(L);
            for (Key<?> key : keys.keys()) {
                app.put(key, appender(key));
            }
            // todo: this does not preserve order
            appenders = Map.copyOf(app);
        }

        private LogEntry entry() {
            return SinkLogging.this.entry().append(", streamKey=").append(streamKey.toString());
        }

        private App appender(Key<?> key) {
            return Objects.requireNonNull(key.type().walk(new AppCreator(key)));
        }

        @Override
        public void ensureRemainingCapacity(long size) {
            entry().append(", ensureRemainingCapacity, size=").append(size).endl();
        }

        @Override
        public void advanceAll() {
            entry().append(", advanceAll").endl();
        }

        // @Override
        // public List<? extends Appender> appenders() {
        // return appenders;
        // }

        @Override
        public Map<Key<?>, ? extends Appender> appendersMap() {
            return appenders;
        }

        private class AppCreator implements Type.Visitor<App>, PrimitiveType.Visitor<App>, GenericType.Visitor<App> {
            private final Key<?> key;

            public AppCreator(Key<?> key) {
                this.key = Objects.requireNonNull(key);
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
                return new ShortLogger(key);
            }

            @Override
            public App visit(IntType intType) {
                return new IntLogger(key);
            }

            @Override
            public App visit(LongType longType) {
                return new LongLogger(key);
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
                return new ObjectApp<>(key, boxedType);
            }

            @Override
            public App visit(StringType stringType) {
                return new ObjectApp<>(key, stringType);
            }

            @Override
            public App visit(InstantType instantType) {
                return new InstantApp(key);
            }

            @Override
            public App visit(ArrayType<?, ?> arrayType) {
                return new ObjectApp<>(key, arrayType);
            }

            @Override
            public App visit(CustomType<?> customType) {
                return new ObjectApp<>(key, customType);
            }
        }

        private abstract class App implements Appender {

            private final Key<?> key;

            App(Key<?> key) {
                this.key = Objects.requireNonNull(key);
            }

            LogEntry entry() {
                return StreamImpl.this.entry()
                        .append(", appender=")
                        .append(key.toString())
                        .append(", type=")
                        .append(type().clazz().getSimpleName());
            }
        }

        private final class ShortLogger extends App implements ShortAppender {
            ShortLogger(Key<?> key) {
                super(key);
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
            IntLogger(Key<?> key) {
                super(key);
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
            LongLogger(Key<?> key) {
                super(key);
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

            ObjectApp(Key<?> key, GenericType<T> type) {
                super(key);
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

            InstantApp(Key<?> key) {
                super(key);
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
