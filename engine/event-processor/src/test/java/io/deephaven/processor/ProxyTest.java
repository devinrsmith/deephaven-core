package io.deephaven.processor;

import io.deephaven.io.log.LogLevel;
import io.deephaven.processor.sink.Proxy;
import io.deephaven.processor.sink.Proxy.Info;
import io.deephaven.processor.sink.Proxy.ObjectTarget;
import io.deephaven.processor.sink.Proxy.StreamingTarget;
import io.deephaven.processor.sink.Sink;
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
        private static final List<Type<?>> ORDER = List.of(Type.instantType(), Type.shortType(), Type.intType(), Type.longType(), Type.stringType());

        private final Stream stream;
        private final ObjectAppender<Instant> myInstant;
        private final ShortAppender myShort;
        private final IntAppender myInt;
        private final LongAppender myLong;
        private final ObjectAppender<String> myString;

        public MyWritingInterfaceManualImpl(Stream stream) {
            this.stream = Objects.requireNonNull(stream);
            myInstant = InstantAppender.get(stream.appenders().get(0));
            myShort = ShortAppender.get(stream.appenders().get(1));
            myInt = IntAppender.get(stream.appenders().get(2));
            myLong = LongAppender.get(stream.appenders().get(3));
            myString = ObjectAppender.get(stream.appenders().get(4), Type.stringType());
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
        final Sink sink = Sinks.strict(Sinks.logging("ProxyTest", LogLevel.INFO, List.of(target.types())));
        final MyWritingInterface p = target.bind(sink.streams().get(0));
        writeTo(p);
        sink.coordinator().sync();
    }

    @Test
    void streamingTargetViaManual() {
        final Sink sink = Sinks.strict(Sinks.logging("ProxyTest", LogLevel.INFO, List.of(MyWritingInterfaceManualImpl.ORDER)));
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
        final Sink sink = Sinks.strict(Sinks.logging("ProxyTest", LogLevel.INFO, List.of(target.types())));
        final Consumer<Pair> consumer = target.bind(sink.streams().get(0));
        consumer.accept(new Pair(1, 2));
        sink.coordinator().sync();
    }
}
