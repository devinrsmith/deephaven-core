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
import io.deephaven.stream.blink.tf.BoxedBooleanFunction;
import io.deephaven.stream.blink.tf.ByteFunction;
import io.deephaven.stream.blink.tf.CharFunction;
import io.deephaven.stream.blink.tf.DoubleFunction;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.ShortFunction;
import io.deephaven.stream.blink.tf.TypedFunction;
import io.deephaven.stream.blink.tf.TypedFunction.Visitor;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

final class BlinkTableMapperImpl<T> implements Producer<T>, StreamPublisher {

    private final TableDefinition definition;
    private final List<Adapter> adapters;
    private final int chunkSize;
    private final Table table;

    private StreamConsumer consumer;
    private WritableChunk<Values>[] chunks;

    BlinkTableMapperImpl(BlinkTableMapperConfig<T> config) {
        this.definition = config.definition();
        this.adapters = new ArrayList<>(config.columns().size());
        this.chunkSize = config.chunkSize();
        int i = 0;
        for (TypedFunction<T> tf : config.columns().values()) {
            adapters.add(new Adapter(tf, i));
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
        private final TypedFunction<T> tf;
        private final int index;
        private Collection<? extends T> values;

        Adapter(TypedFunction<T> tf, int index) {
            this.tf = Objects.requireNonNull(tf);
            this.index = index;
        }

        public void addAll(Collection<? extends T> values) {
            this.values = values;
            try {
                tf.walk(this);
            } finally {
                this.values = null;
            }
        }

        private WritableChunk<Values> chunk() {
            return chunks[index];
        }

        @Override
        public Void visit(CharFunction<T> f) {
            final WritableCharChunk<Values> c = chunk().asWritableCharChunk();
            for (T value : values) {
                c.add(f.applyAsChar(value));
            }
            return null;
        }

        @Override
        public Void visit(ByteFunction<T> f) {
            final WritableByteChunk<Values> c = chunk().asWritableByteChunk();
            for (T value : values) {
                c.add(f.applyAsByte(value));
            }
            return null;
        }

        @Override
        public Void visit(ShortFunction<T> f) {
            final WritableShortChunk<Values> c = chunk().asWritableShortChunk();
            for (T value : values) {
                c.add(f.applyAsShort(value));
            }
            return null;
        }

        @Override
        public Void visit(IntFunction<T> f) {
            final WritableIntChunk<Values> c = chunk().asWritableIntChunk();
            for (T value : values) {
                c.add(f.applyAsInt(value));
            }
            return null;
        }

        @Override
        public Void visit(LongFunction<T> f) {
            final WritableLongChunk<Values> c = chunk().asWritableLongChunk();
            for (T value : values) {
                c.add(f.applyAsLong(value));
            }
            return null;
        }

        @Override
        public Void visit(FloatFunction<T> f) {
            final WritableFloatChunk<Values> c = chunk().asWritableFloatChunk();
            for (T value : values) {
                c.add(f.applyAsFloat(value));
            }
            return null;
        }

        @Override
        public Void visit(DoubleFunction<T> f) {
            final WritableDoubleChunk<Values> c = chunk().asWritableDoubleChunk();
            for (T value : values) {
                c.add(f.applyAsDouble(value));
            }
            return null;
        }

        @Override
        public Void visit(BoxedBooleanFunction<T> f) {
            final WritableByteChunk<Values> c = chunk().asWritableByteChunk();
            for (T value : values) {
                c.add(BooleanUtils.booleanAsByte(f.applyAsBoolean(value)));
            }
            return null;
        }

        @Override
        public Void visit(ObjectFunction<T, ?> f) {
            if (Type.instantType().equals(f.returnType())) {
                // noinspection unchecked
                visitInstant((ObjectFunction<T, Instant>) f);
                return null;
            }
            final WritableObjectChunk<Object, Values> c = chunk().asWritableObjectChunk();
            for (T value : values) {
                c.add(f.apply(value));
            }
            return null;
        }

        private void visitInstant(ObjectFunction<T, Instant> instantMapp) {
            final WritableLongChunk<Values> c = chunk().asWritableLongChunk();
            for (T value : values) {
                c.add(DateTimeUtils.epochNanos(instantMapp.apply(value)));
            }
        }
    }
}
