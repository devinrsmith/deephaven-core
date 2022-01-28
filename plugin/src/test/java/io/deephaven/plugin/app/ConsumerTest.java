package io.deephaven.plugin.app;

import io.deephaven.plugin.app.State.ConsumerBase;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerTest {

    @Test
    void direct() {
        testState(Direct.INSTANCE);
    }

    @Test
    void recursive() {
        testState(Recursive.INSTANCE);
    }

    @Test
    void passThroughDirect() {
        testState(PassThroughDirect.INSTANCE);
    }

    @Test
    void passThroughRecursive() {
        testState(PassThroughRecursive.INSTANCE);
    }

    private static void testState(State state) {
        final MyConsumer consumer = new MyConsumer();
        state.insertInto(consumer);
        assertThat(consumer.set1).isTrue();
        assertThat(consumer.set2).isTrue();
    }

    static class MyConsumer extends ConsumerBase {

        private boolean set1;
        private boolean set2;

        public MyConsumer() {
            super(":");
        }

        @Override
        public void set(String name, Object object) {
            assertThat(set1).isFalse();
            assertThat(name).isEqualTo("a:b:object");
            set1 = true;
        }

        @Override
        public void set(String name, Object object, String description) {
            assertThat(set2).isFalse();
            assertThat(name).isEqualTo("a:d:object");
            set2 = true;
        }
    }

    enum Direct implements State {
        INSTANCE;

        @Override
        public void insertInto(Consumer consumer) {
            final Consumer a = consumer.prefix("a");
            a.prefix("b").set("object", new Object());
            a.prefix("d").set("object", new Object(), "some description");
        }
    }

    enum Recursive implements State {
        INSTANCE;

        @Override
        public void insertInto(Consumer consumer) {
            consumer.set("a", A.INSTANCE);
        }
    }

    enum PassThroughDirect implements State {
        INSTANCE;

        @Override
        public void insertInto(Consumer consumer) {
            consumer.set(Direct.INSTANCE);
        }
    }

    enum PassThroughRecursive implements State {
        INSTANCE;

        @Override
        public void insertInto(Consumer consumer) {
            consumer.set(Recursive.INSTANCE);
        }
    }

    enum A implements State {
        INSTANCE;

        @Override
        public void insertInto(Consumer consumer) {
            consumer.set("b", B.INSTANCE);
            consumer.set("d", D.INSTANCE);
        }
    }

    enum B implements State {
        INSTANCE;

        @Override
        public void insertInto(Consumer consumer) {
            consumer.set("object", new Object());
        }
    }

    enum D implements State {
        INSTANCE;

        @Override
        public void insertInto(Consumer consumer) {
            consumer.set("object", new Object(), "some description");
        }
    }
}
