package io.deephaven.processor;

import io.deephaven.io.log.LogLevel;
import io.deephaven.processor.sink.Proxy;
import io.deephaven.processor.sink.Proxy.ObjectTarget;
import io.deephaven.processor.sink.Proxy.StreamingTarget;
import io.deephaven.processor.sink.Sink;
import io.deephaven.processor.sink.Sinks;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

public class ProxyTest {

    public interface ProxyTestInterface {
        void myInstant(Instant x);

        void myShort(short x);

        void myInt(int x);

        void myLong(long x);

        void myString(String x);

        void advance();
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
    void streamingTarget() {
        final StreamingTarget<ProxyTestInterface> target = Proxy.streamingTarget(ProxyTestInterface.class);
        final Sink sink = Sinks.strict(Sinks.logging("ProxyTest", LogLevel.INFO, List.of(target.types())));
        final ProxyTestInterface p = target.bind(sink.streams().get(0));
        p.myInstant(Instant.now());
        p.myShort((short) 13);
        p.myInt(42);
        p.myLong(43);
        p.myString("hello");
        p.advance();
        sink.coordinator().sync();
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
