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
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.StringType;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.stream.blink.tf.BooleanFunction;
import io.deephaven.stream.blink.tf.ByteFunction;
import io.deephaven.stream.blink.tf.CharFunction;
import io.deephaven.stream.blink.tf.DoubleFunction;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.PrimitiveFunction;
import io.deephaven.stream.blink.tf.ShortFunction;
import io.deephaven.stream.blink.tf.TypedFunction;
import io.deephaven.stream.blink.tf.TypedFunction.Visitor;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static io.deephaven.stream.blink.tf.MapToPrimitives.mapBoolean;
import static io.deephaven.stream.blink.tf.MapToPrimitives.mapByte;
import static io.deephaven.stream.blink.tf.MapToPrimitives.mapChar;
import static io.deephaven.stream.blink.tf.MapToPrimitives.mapDouble;
import static io.deephaven.stream.blink.tf.MapToPrimitives.mapFloat;
import static io.deephaven.stream.blink.tf.MapToPrimitives.mapInstant;
import static io.deephaven.stream.blink.tf.MapToPrimitives.mapInt;
import static io.deephaven.stream.blink.tf.MapToPrimitives.mapLong;
import static io.deephaven.stream.blink.tf.MapToPrimitives.mapShort;

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
        this.chunks = StreamChunkUtils.makeChunksForDefinition(definition, chunkSize);
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
        chunks = StreamChunkUtils.makeChunksForDefinition(definition, chunkSize);
    }

    private class Adapter implements Visitor<T, Void>, PrimitiveFunction.Visitor<T, Void> {
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
        public Void visit(ObjectFunction<T, ?> f) {
            return f.returnType().walk(new ObjectFunctionVisitor(f));
        }

        @Override
        public Void visit(PrimitiveFunction<T> f) {
            return f.walk((PrimitiveFunction.Visitor<T, Void>) this);
        }

        @Override
        public Void visit(BooleanFunction<T> f) {
            return visit(mapBoolean(f));
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

        private <X> void visitGeneric(ObjectFunction<T, X> f) {
            final WritableObjectChunk<X, Values> c = chunk().asWritableObjectChunk();
            for (T value : values) {
                c.add(f.apply(value));
            }
        }

        private class ObjectFunctionVisitor implements GenericType.Visitor<Void>, BoxedType.Visitor<Void> {
            private final ObjectFunction<T, ?> f;

            public ObjectFunctionVisitor(ObjectFunction<T, ?> f) {
                this.f = Objects.requireNonNull(f);
            }

            @Override
            public Void visit(BoxedType<?> boxedType) {
                return boxedType.walk((BoxedType.Visitor<Void>) this);
            }

            @Override
            public Void visit(StringType stringType) {
                visitGeneric(f);
                return null;
            }

            @Override
            public Void visit(InstantType instantType) {
                return Adapter.this.visit(mapInstant(f.asChecked(instantType)));
            }

            @Override
            public Void visit(ArrayType<?, ?> arrayType) {
                visitGeneric(f);
                return null;
            }

            @Override
            public Void visit(CustomType<?> customType) {
                visitGeneric(f);
                return null;
            }

            @Override
            public Void visit(BoxedBooleanType booleanType) {
                return Adapter.this.visit(mapBoolean(f.asChecked(booleanType)));
            }

            @Override
            public Void visit(BoxedByteType byteType) {
                return Adapter.this.visit(mapByte(f.asChecked(byteType)));
            }

            @Override
            public Void visit(BoxedCharType charType) {
                return Adapter.this.visit(mapChar(f.asChecked(charType)));
            }

            @Override
            public Void visit(BoxedShortType shortType) {
                return Adapter.this.visit(mapShort(f.asChecked(shortType)));
            }

            @Override
            public Void visit(BoxedIntType intType) {
                return Adapter.this.visit(mapInt(f.asChecked(intType)));
            }

            @Override
            public Void visit(BoxedLongType longType) {
                return Adapter.this.visit(mapLong(f.asChecked(longType)));
            }

            @Override
            public Void visit(BoxedFloatType floatType) {
                return Adapter.this.visit(mapFloat(f.asChecked(floatType)));
            }

            @Override
            public Void visit(BoxedDoubleType doubleType) {
                return Adapter.this.visit(mapDouble(f.asChecked(doubleType)));
            }
        }
    }

    @Override
    public void shutdown() {
        // todo
    }
}
