//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor;

import io.deephaven.io.log.LogLevel;
import io.deephaven.processor.sink.Coordinator;
import io.deephaven.processor.sink.Key;
import io.deephaven.processor.sink.Keys;
import io.deephaven.processor.sink.Sink;
import io.deephaven.processor.sink.Sink.StreamKey;
import io.deephaven.processor.sink.Sinks;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.IntAppender;
import io.deephaven.qst.type.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class SinkStrictTest {

    private static final StreamKey KEY = new StreamKey();
    private static final Key<Integer> A0 = Key.of("A0", Type.intType());
    private static final Key<Integer> A1 = Key.of("A1", Type.intType());


    private Sink sink;
    private Coordinator coordinator;
    private Stream s1;
    private IntAppender a0;
    private IntAppender a1;


    @BeforeEach
    void setUp() {
        sink = Sinks.strict(Sinks.logging(SinkStrictTest.class.getSimpleName(), LogLevel.INFO,
                Map.of(KEY, Keys.builder().addKeys(A0, A1).build())));
        coordinator = sink.coordinator();
        s1 = Sink.get(sink, KEY);
        a0 = IntAppender.get(s1, A0);
        a1 = IntAppender.get(s1, A1);
    }

    @Test
    void notWritingSet() {
        try {
            a0.set(42);
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("writing");
        }
    }

    @Test
    void notWritingAdvance() {
        try {
            a0.advance();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("writing");
        }
    }

    @Test
    void notWritingSetNull() {
        try {
            a0.setNull();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("writing");
        }
    }

    @Test
    void notWritingSync() {
        try {
            coordinator.sync();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("writing");
        }
    }

    @Test
    void doubleWriting() {
        coordinator.writing();
        try {
            coordinator.writing();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("writing");
        }
    }

    @Test
    void notWritingEnsureRemainingCapacity() {
        try {
            s1.ensureRemainingCapacity(1L);
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("writing");
        }
    }

    @Test
    void notWritingAdvanceAll() {
        try {
            s1.advanceAll();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("writing");
        }
    }

    @Test
    void setWithoutAdvance() {
        coordinator.writing();
        s1.ensureRemainingCapacity(1);
        a0.set(42);
        a1.set(43);
        try {
            coordinator.sync();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("not advanced");
        }
    }

    @Test
    void missingSet() {
        coordinator.writing();
        s1.ensureRemainingCapacity(1);
        a0.set(42);
        // todo: should we allow the absence of setting meaning an implicit null?
        try {
            s1.advanceAll();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Must ensure all appenders have been set before advanceAll");
        }
    }

    @Test
    void noSet() {
        coordinator.writing();
        s1.ensureRemainingCapacity(1);
        // todo: should we allow the absence of setting meaning an implicit null?
        try {
            s1.advanceAll();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Must ensure all appenders have been set before advanceAll");
        }
    }

    @Test
    void correctUsage() {
        coordinator.writing();
        s1.ensureRemainingCapacity(2);
        a0.set(1);
        a1.set(2);
        s1.advanceAll();

        a0.set(3);
        a1.set(4);
        s1.advanceAll();
        coordinator.sync();
    }
}
