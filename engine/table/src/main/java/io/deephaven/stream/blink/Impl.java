package io.deephaven.stream.blink;

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
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.stream.blink.Mapp.Visitor;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

final class Impl<T> implements Producer<T>, StreamPublisher {

    private final TableDefinition definition;
    private final List<Adapter> adapters;
    private final int chunkSize;
    private final Table table;

    private StreamConsumer consumer;
    private WritableChunk<Values>[] chunks;

    Impl(BlinkTableMapperConfig<T> config) {
        this.definition = config.definition();
        this.adapters = new ArrayList<>(config.map().size());
        this.chunkSize = config.chunkSize();
        int i = 0;
        for (Mapp<T> mapp : config.map().values()) {
            adapters.add(new Adapter(mapp, i));
            ++i;
        }
        final StreamToBlinkTableAdapter adapter =
                new StreamToBlinkTableAdapter(definition, this, config.updateSourceRegistrar(), config.name());
        this.table = adapter.table();
        this.chunks = StreamToBlinkTableAdapter.makeChunksForDefinition(definition, chunkSize);
    }

    public Table table() {
        return table;
    }

    // Producer impl

    @Override
    public synchronized void add(T value) {
        addAll(List.of(value));
    }

    @Override
    public synchronized void addAll(Collection<? extends T> values) {
        // This is a strategy that batches _up_ to chunkSize
        // There may be other strategies we want to consider in the future, such as growing the chunkSize
        List<? extends T> remaining = List.copyOf(values);
        int chunkFree;
        while (remaining.size() >= (chunkFree = chunkSize - chunks[0].size())) {
            final List<? extends T> sub = remaining.subList(0, chunkFree);
            for (Adapter adapter : adapters) {
                adapter.addAll(sub);
            }
            flushInternal();
            remaining = remaining.subList(chunkFree, remaining.size());
        }
        for (Adapter adapter : adapters) {
            adapter.addAll(remaining);
        }
        // no need to flush, we knew that remaining.size() < chunkFree
    }

    @Override
    public void failure(Throwable t) {
        consumer.acceptFailure(t);
    }

    // Stream publisher

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public synchronized void flush() {
        if (chunks[0].size() == 0) {
            return;
        }
        flushInternal();
    }

    private void flushInternal() {
        consumer.accept(chunks);
        chunks = StreamToBlinkTableAdapter.makeChunksForDefinition(definition, chunkSize);
    }

    private class Adapter implements Visitor<T, Void> {
        private final Mapp<T> mapp;
        private final int index;
        private Collection<? extends T> values;

        Adapter(Mapp<T> mapp, int index) {
            this.mapp = Objects.requireNonNull(mapp);
            this.index = index;
        }

        public void addAll(Collection<? extends T> values) {
            this.values = values;
            try {
                mapp.walk(this);
            } finally {
                this.values = null;
            }
        }

        private WritableChunk<Values> chunk() {
            return chunks[index];
        }

        @Override
        public Void visit(CharMapp<T> charMapp) {
            final WritableCharChunk<Values> c = chunk().asWritableCharChunk();
            for (T value : values) {
                c.add(charMapp.applyAsChar(value));
            }
            return null;
        }

        @Override
        public Void visit(ByteMapp<T> byteMapp) {
            final WritableByteChunk<Values> c = chunk().asWritableByteChunk();
            for (T value : values) {
                c.add(byteMapp.applyAsByte(value));
            }
            return null;
        }

        @Override
        public Void visit(ShortMapp<T> shortMapp) {
            final WritableShortChunk<Values> c = chunk().asWritableShortChunk();
            for (T value : values) {
                c.add(shortMapp.applyAsShort(value));
            }
            return null;
        }

        @Override
        public Void visit(IntMapp<T> intMapp) {
            final WritableIntChunk<Values> c = chunk().asWritableIntChunk();
            for (T value : values) {
                c.add(intMapp.applyAsInt(value));
            }
            return null;
        }

        @Override
        public Void visit(LongMapp<T> longMapp) {
            final WritableLongChunk<Values> c = chunk().asWritableLongChunk();
            for (T value : values) {
                c.add(longMapp.applyAsLong(value));
            }
            return null;
        }

        @Override
        public Void visit(FloatMapp<T> floatMapp) {
            final WritableFloatChunk<Values> c = chunk().asWritableFloatChunk();
            for (T value : values) {
                c.add(floatMapp.applyAsFloat(value));
            }
            return null;
        }

        @Override
        public Void visit(DoubleMapp<T> doubleMapp) {
            final WritableDoubleChunk<Values> c = chunk().asWritableDoubleChunk();
            for (T value : values) {
                c.add(doubleMapp.applyAsDouble(value));
            }
            return null;
        }

        @Override
        public Void visit(BooleanMapp<T> booleanMapp) {
            final WritableByteChunk<Values> c = chunk().asWritableByteChunk();
            for (T value : values) {
                c.add(booleanMapp.applyAsBoolean(value) ? (byte) 1 : (byte) 0);
            }
            return null;
        }

        @Override
        public Void visit(ObjectMapp<T, ?> objectMapp) {
            if (Type.instantType().equals(objectMapp.returnType())) {
                // noinspection unchecked
                visitInstant((ObjectMapp<T, Instant>) objectMapp);
                return null;
            }
            final WritableObjectChunk<Object, Values> c = chunk().asWritableObjectChunk();
            for (T value : values) {
                c.add(objectMapp.apply(value));
            }
            return null;
        }

        private void visitInstant(ObjectMapp<T, Instant> instantMapp) {
            final WritableLongChunk<Values> c = chunk().asWritableLongChunk();
            for (T value : values) {
                c.add(DateTimeUtils.epochNanos(instantMapp.apply(value)));
            }
        }
    }
}
