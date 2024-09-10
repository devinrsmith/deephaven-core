package io.deephaven.processor;

import io.deephaven.io.log.LogLevel;
import io.deephaven.processor.sink.Coordinator;
import io.deephaven.processor.sink.Sink;
import io.deephaven.processor.sink.Sinks;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.IntAppender;
import io.deephaven.qst.type.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class SinkStrictTest {

    private Sink sink;
    private Coordinator coordinator;
    private Stream stream;
    private IntAppender a0;
    private IntAppender a1;

    @BeforeEach
    void setUp() {
        sink = Sinks.strict(Sinks.logging(SinkStrictTest.class.getSimpleName(), LogLevel.INFO, List.of(List.of(Type.intType(), Type.intType()))));
        coordinator = sink.coordinator();
        stream = sink.streams().get(0);
        a0 = IntAppender.get(stream.appenders().get(0));
        a1 = IntAppender.get(stream.appenders().get(1));
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
        try{
            coordinator.writing();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("writing");
        }
    }

    @Test
    void notWritingEnsureRemainingCapacity() {
        try {
            stream.ensureRemainingCapacity(1L);
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("writing");
        }
    }

    @Test
    void notWritingAdvanceAll() {
        try {
            stream.advanceAll();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("writing");
        }
    }

    @Test
    void setWithoutAdvance() {
        coordinator.writing();
        stream.ensureRemainingCapacity(1);
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
        stream.ensureRemainingCapacity(1);
        a0.set(42);
        // todo: should we allow the absence of setting meaning an implicit null?
        try {
            stream.advanceAll();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Must ensure all appenders have been set before advanceAll");
        }
    }

    @Test
    void noSet() {
        coordinator.writing();
        stream.ensureRemainingCapacity(1);
        // todo: should we allow the absence of setting meaning an implicit null?
        try {
            stream.advanceAll();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("Must ensure all appenders have been set before advanceAll");
        }
    }

    @Test
    void correctUsage() {
        coordinator.writing();
        stream.ensureRemainingCapacity(2);
        a0.set(1);
        a1.set(2);
        stream.advanceAll();
        a0.set(3);
        a1.set(4);
        stream.advanceAll();
        coordinator.sync();
    }
}
