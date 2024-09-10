package io.deephaven.stream;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.processor.sink.Coordinator;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.Appender;
import io.deephaven.processor.sink.appender.DoubleAppender;
import io.deephaven.processor.sink.appender.IntAppender;
import io.deephaven.util.QueryConstants;

import java.util.List;

final class Chunks implements Stream, Coordinator, StreamPublisher {

//    private final WritableChunk<?> chunk;

    private final List<Base> appenders;
    private WritableChunk<Values>[] chunks;

    private int pos;

    @Override
    public void ensureRemainingCapacity(long size) {

        // todo
    }

    @Override
    public List<? extends Appender> appenders() {
        return List.copyOf(appenders);
    }

    @Override
    public void advanceAll() {
        ++pos;
        // todo: handoff, ensure remaining capacity?
    }

    //

    @Override
    public void sync() {

    }

    @Override
    public void flush() {
        // can only publish data that has been reached as of a sync point
    }

    void setChunks(WritableChunk<Values>[] chunks) {
        // todo: ensure all offsets are equal
        if (chunks.length != appenders.size()) {
            throw new IllegalStateException();
        }
        this.chunks = chunks;
        for (int i = 0; i < chunks.length; ++i) {
            appenders.get(i).setChunk(chunks[i]);
        }
        // todo: use offset to avoid addition?
        pos = 0;
    }

    private static abstract class Base implements Appender {

        abstract void setChunk(WritableChunk<Values> chunk);

        public final void advance() {
            throw new UnsupportedOperationException();
        }
    }

    private final class Int extends Base implements IntAppender {
        private WritableIntChunk<Values> chunk;

        @Override
        void setChunk(WritableChunk<Values> chunk) {
            this.chunk = (WritableIntChunk<Values>) chunk;
        }

        @Override
        public void setNull() {
            chunk.set(pos, QueryConstants.NULL_INT);
        }

        @Override
        public void set(int value) {
            chunk.set(pos, value);
        }
    }

    private final class Double extends Base implements DoubleAppender {
        private WritableDoubleChunk<Values> chunk;

        @Override
        void setChunk(WritableChunk<Values> chunk) {
            this.chunk = (WritableDoubleChunk<Values>) chunk;
        }

        @Override
        public void setNull() {
            chunk.set(pos, QueryConstants.NULL_DOUBLE);
        }

        @Override
        public void set(double value) {
            chunk.set(pos, value);
        }
    }
}
