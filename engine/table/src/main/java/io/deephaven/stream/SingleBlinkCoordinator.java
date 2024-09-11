//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stream;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.processor.sink.Coordinator;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.Appender;
import io.deephaven.processor.sink.appender.ByteAppender;
import io.deephaven.processor.sink.appender.CharAppender;
import io.deephaven.processor.sink.appender.DoubleAppender;
import io.deephaven.processor.sink.appender.FloatAppender;
import io.deephaven.processor.sink.appender.InstantAppender;
import io.deephaven.processor.sink.appender.InstantUtils;
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
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;

public final class SingleBlinkCoordinator implements Stream, Coordinator, StreamPublisher {

    // private final WritableChunk<?> chunk;

    private final int chunkSize;
    private final List<Base> appenders;
    private final WritableChunk[] space;
    private final List<WritableChunk<Values>[]> space2;

    // don't care about re-entrancy; but maybe fairness?
    // private final Lock lock = new ReentrantLock(true);
    // private final Lock lock = new ReentrantLock(false);
    private final Lock lock = new StampedLock().asWriteLock();

    // todo: do we need fairness? want to make sure flush() gets priority
    private StreamConsumer consumer;

    private int pos;

    public SingleBlinkCoordinator(List<Type<?>> types) {
        chunkSize = 1024;
        appenders = new ArrayList<>();
        final int L = types.size();
        for (int i = 0; i < L; ++i) {
            appenders.add(appender(types.get(i), i));
        }
        space = new WritableChunk[appenders.size()];
        //noinspection unchecked
        space2 = List.<WritableChunk<Values>[]>of(space);
    }

    private Base appender(Type<?> type, int index) {
        return Objects.requireNonNull(type.walk(new BaseV(index)));
    }

    @Override
    public void writing() {
        lock.lock();
    }

    @Override
    public void sync() {
        lock.unlock();
    }

    //

    @Override
    public void ensureRemainingCapacity(long size) {
        // ignore? we check every advanceAll...?
        // final int remainingCapacity = chunkSize - size();
        // if (size > remainingCapacity) {
        //
        // }
    }

    @Override
    public List<? extends Appender> appenders() {
        return List.copyOf(appenders);
    }

    @Override
    public void advanceAll() {
        ++pos;
        if (isFull()) {
            flushImpl();
        }
    }

    //

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        this.consumer = Objects.requireNonNull(consumer);
    }

    @Override
    public void flush() {
        // todo: timeout? tryLock? semantics?
        // we could argue skipping flushing if the lock is taken, but we can _only_ do that if no unsynced data has been
        // flushed (as might happen if we have a very large list that needs to be synced, but was flushed in the interim
        // to create new buffers).
        lock.lock();
        try {
            flushImpl();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown() {

    }

    //

    private boolean isFull() {
        return size() == chunkSize;
    }

    private int size() {
        return pos; // todo wrt offset?
    }

    private void flushImpl() {
        final int size = size();
        if (size == 0) {
            return;
        }
        for (Base appender : appenders) {
            appender.swap();
        }
        // noinspection unchecked
//        final WritableChunk<Values>[] chunks = appenders.stream()
//                .map(Base::take)
//                .toArray(WritableChunk[]::new);
//        for (WritableChunk<Values> chunk : chunks) {
//            chunk.setSize(size);
//        }
        pos = 0;
        consumer.accept(space2);
    }

    private class BaseV implements Type.Visitor<Base>, PrimitiveType.Visitor<Base>, GenericType.Visitor<Base> {
        private final int index;

        public BaseV(int index) {
            this.index = index;
        }

        @Override
        public Base visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<Base>) this);
        }

        @Override
        public Base visit(GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<Base>) this);
        }

        @Override
        public Base visit(BooleanType booleanType) {
            // todo byte stuff
            return null;
        }

        @Override
        public Base visit(ByteType byteType) {
            return new Byte(index);
        }

        @Override
        public Base visit(CharType charType) {
            return new Char(index);
        }

        @Override
        public Base visit(ShortType shortType) {
            return new Short(index);
        }

        @Override
        public Base visit(IntType intType) {
            return new Int(index);
        }

        @Override
        public Base visit(LongType longType) {
            return new Long(index);
        }

        @Override
        public Base visit(FloatType floatType) {
            return new Float(index);
        }

        @Override
        public Base visit(DoubleType doubleType) {
            return new Double(index);
        }

        @Override
        public Base visit(BoxedType<?> boxedType) {
            return boxedType.primitiveType().walk((PrimitiveType.Visitor<Base>) this);
        }

        @Override
        public Base visit(StringType stringType) {
            return new Obj<>(stringType, index);
        }

        @Override
        public Base visit(InstantType instantType) {
            return new Inst(index);
        }

        @Override
        public Base visit(ArrayType<?, ?> arrayType) {
            return new Obj<>(arrayType, index);
        }

        @Override
        public Base visit(CustomType<?> customType) {
            return new Obj<>(customType, index);
        }
    }

    private abstract class Base implements Appender {

        final int index;

        Base(int index) {
            this.index = index;
            take();
        }

        abstract WritableChunk<Values> take();

        void swap() {
            final WritableChunk<Values> chunk = take();
            chunk.setSize(pos);
            space[index] = chunk;
        }

        public final void advance() {
            throw new UnsupportedOperationException();
        }
    }

    private final class Char extends Base implements CharAppender {
        private WritableCharChunk<Values> chunk;

        public Char(int index) {
            super(index);
        }

        @Override
        WritableCharChunk<Values> take() {
            try {
                return chunk;
            } finally {
                chunk = WritableCharChunk.makeWritableChunk(chunkSize);
            }
        }

        @Override
        public void setNull() {
            chunk.set(pos, QueryConstants.NULL_CHAR);
        }

        @Override
        public void set(char value) {
            chunk.set(pos, value);
        }
    }

    private final class Byte extends Base implements ByteAppender {
        private WritableByteChunk<Values> chunk;

        public Byte(int index) {
            super(index);
        }

        @Override
        WritableByteChunk<Values> take() {
            try {
                return chunk;
            } finally {
                chunk = WritableByteChunk.makeWritableChunk(chunkSize);
            }
        }

        @Override
        public void setNull() {
            chunk.set(pos, QueryConstants.NULL_BYTE);
        }

        @Override
        public void set(byte value) {
            chunk.set(pos, value);
        }
    }

    private final class Short extends Base implements ShortAppender {
        private WritableShortChunk<Values> chunk;

        public Short(int index) {
            super(index);
        }

        @Override
        WritableShortChunk<Values> take() {
            try {
                return chunk;
            } finally {
                chunk = WritableShortChunk.makeWritableChunk(chunkSize);
            }
        }

        @Override
        public void setNull() {
            chunk.set(pos, QueryConstants.NULL_SHORT);
        }

        @Override
        public void set(short value) {
            chunk.set(pos, value);
        }
    }

    private final class Int extends Base implements IntAppender {
        private WritableIntChunk<Values> chunk;

        public Int(int index) {
            super(index);
        }

        @Override
        WritableIntChunk<Values> take() {
            try {
                return chunk;
            } finally {
                chunk = WritableIntChunk.makeWritableChunk(chunkSize);
            }
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

    private final class Long extends Base implements LongAppender {
        private WritableLongChunk<Values> chunk;

        public Long(int index) {
            super(index);
        }

        @Override
        WritableLongChunk<Values> take() {
            try {
                return chunk;
            } finally {
                chunk = WritableLongChunk.makeWritableChunk(chunkSize);
            }
        }

        @Override
        public void setNull() {
            chunk.set(pos, QueryConstants.NULL_LONG);
        }

        @Override
        public void set(long value) {
            chunk.set(pos, value);
        }
    }

    private final class Float extends Base implements FloatAppender {
        private WritableFloatChunk<Values> chunk;

        public Float(int index) {
            super(index);
        }

        @Override
        WritableFloatChunk<Values> take() {
            try {
                return chunk;
            } finally {
                chunk = WritableFloatChunk.makeWritableChunk(chunkSize);
            }
        }

        @Override
        public void setNull() {
            chunk.set(pos, QueryConstants.NULL_FLOAT);
        }

        @Override
        public void set(float value) {
            chunk.set(pos, value);
        }
    }

    private final class Double extends Base implements DoubleAppender {
        private WritableDoubleChunk<Values> chunk;

        public Double(int index) {
            super(index);
        }

        @Override
        WritableDoubleChunk<Values> take() {
            try {
                return chunk;
            } finally {
                chunk = WritableDoubleChunk.makeWritableChunk(chunkSize);
            }
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

    private final class Obj<T> extends Base implements ObjectAppender<T> {
        private final GenericType<T> type;
        private WritableObjectChunk<T, Values> chunk;

        Obj(GenericType<T> type, int index) {
            super(index);
            this.type = Objects.requireNonNull(type);
        }

        @Override
        WritableObjectChunk<T, Values> take() {
            try {
                return chunk;
            } finally {
                chunk = WritableObjectChunk.makeWritableChunk(chunkSize);
            }
        }

        @Override
        public GenericType<T> type() {
            return type;
        }

        @Override
        public void setNull() {
            chunk.set(pos, null);
        }

        @Override
        public void set(T value) {
            chunk.set(pos, value);
        }
    }

    private final class Inst extends Base implements InstantAppender {
        private WritableLongChunk<Values> chunk;

        public Inst(int index) {
            super(index);
        }

        @Override
        WritableLongChunk<Values> take() {
            try {
                return chunk;
            } finally {
                chunk = WritableLongChunk.makeWritableChunk(chunkSize);
            }
        }

        @Override
        public LongAppender asLongEpochAppender(TimeUnit unit) {
            return InstantUtils.asEpochLong(new Nanos(), unit);
        }

        @Override
        public DoubleAppender asDoubleEpochAppender(TimeUnit unit, RoundingMode roundingMode) {
            return InstantUtils.asEpochDouble(asLongEpochAppender(unit), unit, roundingMode);
        }

        @Override
        public ObjectAppender<String> asStringEpochConsumer(TimeUnit unit, RoundingMode roundingMode) {
            return InstantUtils.asEpochString(asLongEpochAppender(unit), unit, roundingMode);
        }

        @Override
        public GenericType<Instant> type() {
            return Type.instantType();
        }

        @Override
        public void setNull() {
            chunk.set(pos, QueryConstants.NULL_LONG);
        }

        @Override
        public void set(Instant value) {
            chunk.set(pos, DateTimeUtils.epochNanos(value));
        }

        private class Nanos implements LongAppender {
            @Override
            public void setNull() {
                chunk.set(pos, QueryConstants.NULL_LONG);
            }

            @Override
            public void set(long value) {
                chunk.set(pos, value);
            }

            @Override
            public void advance() {
                Inst.this.advance();
            }
        }
    }
}
