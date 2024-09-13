//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor;

import io.deephaven.io.log.LogLevel;
import io.deephaven.processor.sink.Key;
import io.deephaven.processor.sink.Keys;
import io.deephaven.processor.sink.Proxy;
import io.deephaven.processor.sink.Proxy.Info;
import io.deephaven.processor.sink.Proxy.ObjectTarget;
import io.deephaven.processor.sink.Proxy.StreamingTarget;
import io.deephaven.processor.sink.Sink;
import io.deephaven.processor.sink.Sink.StreamKey;
import io.deephaven.processor.sink.Sinks;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.InstantAppender;
import io.deephaven.processor.sink.appender.IntAppender;
import io.deephaven.processor.sink.appender.LongAppender;
import io.deephaven.processor.sink.appender.ObjectAppender;
import io.deephaven.processor.sink.appender.ShortAppender;
import io.deephaven.qst.type.Type;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class ProxyTest {

    public interface MyWritingInterface {
        // todo: nullable annotation?
        @Info(index = 0)
        void myInstant(Instant x);

        @Info(index = 1)
        void myShort(short x);

        @Info(index = 2)
        void myInt(int x);

        @Info(index = 3)
        void myLong(long x);

        @Info(index = 4)
        void myString(String x);

        void advance();
    }

    private static class MyWritingInterfaceManualImpl implements MyWritingInterface {
        private static final List<Type<?>> ORDER =
                List.of(Type.instantType(), Type.shortType(), Type.intType(), Type.longType(), Type.stringType());

        private static final Key<Instant> INSTANT = Key.of("myInstant", Type.instantType());
        private static final Key<Short> SHORT = Key.of("myShort", Type.shortType());
        private static final Key<Integer> INT = Key.of("myInt", Type.intType());
        private static final Key<Long> LONG = Key.of("myLong", Type.longType());
        private static final Key<String> STRING = Key.of("myString", Type.stringType());

        private static final Map<StreamKey, Keys> MAP = Map.of(new StreamKey(), Keys.builder()
                .addKeys(INSTANT)
                .build());

        private final Stream stream;
        private final ObjectAppender<Instant> myInstant;
        private final ShortAppender myShort;
        private final IntAppender myInt;
        private final LongAppender myLong;
        private final ObjectAppender<String> myString;

        public MyWritingInterfaceManualImpl(Stream stream) {
            this.stream = Objects.requireNonNull(stream);
            myInstant = InstantAppender.get(stream, INSTANT);
            myShort = ShortAppender.get(stream, SHORT);
            myInt = IntAppender.get(stream, INT);
            myLong = LongAppender.get(stream, LONG);
            myString = ObjectAppender.get(stream, STRING);
        }

        @Override
        public void myInstant(Instant x) {
            if (x == null) {
                myInstant.setNull();
            } else {
                myInstant.set(x);
            }
        }

        @Override
        public void myShort(short x) {
            myShort.set(x);
        }

        @Override
        public void myInt(int x) {
            myInt.set(x);
        }

        @Override
        public void myLong(long x) {
            myLong.set(x);
        }

        @Override
        public void myString(String x) {
            if (x == null) {
                myString.setNull();
            } else {
                myString.set(x);
            }
        }

        @Override
        public void advance() {
            stream.advanceAll();
        }
    }

    public static class Pair {
        private final int x;
        private final int y;

        public Pair(int x, int y) {
            this.x = x;
            this.y = y;
        }

        public int x() {
            return x;
        }

        public int y() {
            return y;
        }
    }

    @Test
    void streamingTargetViaProxy() {
        final StreamingTarget<MyWritingInterface> target = Proxy.streamingTarget(MyWritingInterface.class);
        final Sink sink = Sinks.strict(Sinks.logging("ProxyTest", LogLevel.INFO, Map.of(target.key(), target.keys())));
        final MyWritingInterface p = target.bind(Sink.get(sink, target.key()));
        writeTo(p);
        sink.coordinator().sync();
    }

    @Test
    void streamingTargetViaManual() {
        final Sink sink =
                Sinks.strict(Sinks.logging("ProxyTest", LogLevel.INFO, MyWritingInterfaceManualImpl.MAP));
        final MyWritingInterface p = new MyWritingInterfaceManualImpl(sink.streams().get(0));
        writeTo(p);
        sink.coordinator().sync();
    }

    private static void writeTo(MyWritingInterface p) {
        p.myInstant(Instant.now());
        p.myShort((short) 13);
        p.myInt(42);
        p.myLong(43);
        p.myString("hello");
        p.advance();
    }

    @Test
    void objectTarget() {
        final ObjectTarget<Pair> target = Proxy.objectTarget(Pair.class);
        final Sink sink = Sinks.strict(Sinks.logging("ProxyTest", LogLevel.INFO, Map.of(target.key(), target.keys())));
        final Consumer<Pair> consumer = target.bind(Sink.get(sink, target.key()));
        consumer.accept(new Pair(1, 2));
        sink.coordinator().sync();
    }
}
