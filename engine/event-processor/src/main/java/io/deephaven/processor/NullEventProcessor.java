package io.deephaven.processor;

import io.deephaven.processor.appender.Appender;
import io.deephaven.processor.appender.BooleanAppender;
import io.deephaven.processor.appender.ByteAppender;
import io.deephaven.processor.appender.CharAppender;
import io.deephaven.processor.appender.DoubleAppender;
import io.deephaven.processor.appender.FloatAppender;
import io.deephaven.processor.appender.InstantAppender;
import io.deephaven.processor.appender.IntAppender;
import io.deephaven.processor.appender.LongAppender;
import io.deephaven.processor.appender.ObjectAppender;
import io.deephaven.processor.appender.ShortAppender;
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
import io.deephaven.qst.type.Type.Visitor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class NullEventProcessor<T> implements EventProcessor<T> {

    public static <X> EventProcessor<X> of(List<StreamSpec> specs, Random random) {
        final List<BiConsumer<Stream, Coordinator>> out = new ArrayList<>(specs.size());
        for (StreamSpec spec : specs) {
            out.add(spec.isRowOriented()
                    ? RowOrientedNullProducer.of(spec, random)
                    : ColumnOrientedNullProducer.of(spec, random));
        }
        return new NullEventProcessor<>(specs, out);
    }

    private final List<StreamSpec> specs;
    private final List<BiConsumer<Stream, Coordinator>> consumers;

    private NullEventProcessor(List<StreamSpec> specs, List<BiConsumer<Stream, Coordinator>> consumers) {
        this.specs = Objects.requireNonNull(specs);
        this.consumers = Objects.requireNonNull(consumers);
    }

    @Override
    public EventSpec spec() {
        return new EventSpec() {
            @Override
            public List<StreamSpec> streams() {
                return specs;
            }

            @Override
            public boolean usesCoordinator() {
                return true;
            }
        };
    }

    @Override
    public void process(T event, List<Stream> streams, Coordinator coordinator) {
        final Iterator<BiConsumer<Stream, Coordinator>> consumerIt = consumers.iterator();
        final Iterator<Stream> streamIt = streams.iterator();
        while (consumerIt.hasNext()) {
            if (!streamIt.hasNext()) {
                throw new IllegalStateException();
            }
            consumerIt.next().accept(streamIt.next(), coordinator);
        }
        if (streamIt.hasNext()) {
            throw new IllegalStateException();
        }
    }

    private static int expectedSize(StreamSpec spec, Random random) {
        return Math.toIntExact(spec.expectedSize().orElseGet(() -> random.nextInt(128)));
    }

    private static class ColumnOrientedNullProducer implements BiConsumer<Stream, Coordinator> {
        public static ColumnOrientedNullProducer of(StreamSpec spec, Random random) {
            if (spec.isRowOriented()) {
                throw new IllegalArgumentException();
            }
            final List<Type<?>> types = spec.outputTypes();
            final List<Consumer<Appender>> consumers = new ArrayList<>(types.size());
            final ColumnOrientedNullAppender columnOrientedNullAppender = new ColumnOrientedNullAppender();
            for (Type<?> type : types) {
                consumers.add(type.walk(columnOrientedNullAppender));
            }
            return new ColumnOrientedNullProducer(columnOrientedNullAppender, consumers, random, expectedSize(spec, random));
        }

        private final ColumnOrientedNullAppender columnOrientedNullAppender;
        private final List<Consumer<Appender>> columnOrientedConsumers;
        private final Random random;
        private final int expectedSize;

        private ColumnOrientedNullProducer(ColumnOrientedNullAppender columnOrientedNullAppender, List<Consumer<Appender>> columnOrientedConsumers, Random random, int expectedSize) {
            this.columnOrientedNullAppender = Objects.requireNonNull(columnOrientedNullAppender);
            this.columnOrientedConsumers = Objects.requireNonNull(columnOrientedConsumers);
            this.random = Objects.requireNonNull(random);
            this.expectedSize = expectedSize;
        }

        @Override
        public void accept(Stream stream, Coordinator coordinator) {
            final int actualSize = random.nextInt(Math.multiplyExact(expectedSize, 2));
            if (actualSize == 0) {
                return;
            }
            columnOrientedNullAppender.setTimes(actualSize);
            stream.ensureRemainingCapacity(actualSize);
            final Iterator<Consumer<Appender>> consumerIt = columnOrientedConsumers.iterator();
            final Iterator<Appender> appenderIt = stream.appenders().iterator();
            while (consumerIt.hasNext()) {
                if (!appenderIt.hasNext()) {
                    throw new IllegalStateException();
                }
                consumerIt.next().accept(appenderIt.next());
            }
            if (appenderIt.hasNext()) {
                throw new IllegalStateException();
            }
            coordinator.sync();
        }
    }

    private static class RowOrientedNullProducer implements BiConsumer<Stream, Coordinator> {

        public static RowOrientedNullProducer of(StreamSpec spec, Random random) {
            if (!spec.isRowOriented()) {
                throw new IllegalArgumentException();
            }
            final List<Type<?>> types = spec.outputTypes();
            final List<Consumer<Appender>> setters = new ArrayList<>(types.size());
            for (Type<?> type : types) {
                setters.add(RowOrientedNullSetter.of(type));
            }
            return new RowOrientedNullProducer(expectedSize(spec, random), setters, random);
        }

        private final int expectedSize;
        private final List<Consumer<Appender>> rowOrientedConsumers;
        private final Random random;

        private RowOrientedNullProducer(int expectedSize, List<Consumer<Appender>> rowOrientedConsumers, Random random) {
            this.expectedSize = Math.toIntExact(expectedSize);
            this.rowOrientedConsumers = Objects.requireNonNull(rowOrientedConsumers);
            this.random = Objects.requireNonNull(random);
        }

        @Override
        public void accept(Stream stream, Coordinator coordinator) {
            final int actualSize = random.nextInt(Math.multiplyExact(expectedSize, 2));
            if (actualSize == 0) {
                return;
            }
            stream.ensureRemainingCapacity(actualSize);
            for (int i = 0; i < actualSize; i++) {
                final Iterator<Consumer<Appender>> consumerIt = rowOrientedConsumers.iterator();
                final Iterator<Appender> appenderIt = stream.appenders().iterator();
                while (consumerIt.hasNext()) {
                    if (!appenderIt.hasNext()) {
                        throw new IllegalStateException();
                    }
                    consumerIt.next().accept(appenderIt.next());
                }
                if (appenderIt.hasNext()) {
                    throw new IllegalStateException();
                }
                stream.advanceAll();
                coordinator.sync();
            }
        }
    }

    private enum RowOrientedNullSetter implements Visitor<Consumer<Appender>>, PrimitiveType.Visitor<Consumer<Appender>>, GenericType.Visitor<Consumer<Appender>> {
        INSTANCE;

        public static Consumer<Appender> of(Type<?> type) {
            return type.walk(INSTANCE);
        }

        @Override
        public Consumer<Appender> visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<Consumer<Appender>>) this);
        }

        @Override
        public Consumer<Appender> visit(GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<Consumer<Appender>>) this);
        }

        @Override
        public Consumer<Appender> visit(BooleanType booleanType) {
            return RowOrientedNullSetter::setNullBoolean;
        }

        @Override
        public Consumer<Appender> visit(ByteType byteType) {
            return RowOrientedNullSetter::setNullByte;
        }

        @Override
        public Consumer<Appender> visit(CharType charType) {
            return RowOrientedNullSetter::setNullChar;
        }

        @Override
        public Consumer<Appender> visit(ShortType shortType) {
            return RowOrientedNullSetter::setNullShort;
        }

        @Override
        public Consumer<Appender> visit(IntType intType) {
            return RowOrientedNullSetter::setNullInt;
        }

        @Override
        public Consumer<Appender> visit(LongType longType) {
            return RowOrientedNullSetter::setNullLong;
        }

        @Override
        public Consumer<Appender> visit(FloatType floatType) {
            return RowOrientedNullSetter::setNullFloat;
        }

        @Override
        public Consumer<Appender> visit(DoubleType doubleType) {
            return RowOrientedNullSetter::setNullDouble;
        }

        @Override
        public Consumer<Appender> visit(BoxedType<?> boxedType) {
            return RowOrientedNullSetter::setNullObject;
        }

        @Override
        public Consumer<Appender> visit(StringType stringType) {
            return RowOrientedNullSetter::setNullObject;
        }

        @Override
        public Consumer<Appender> visit(InstantType instantType) {
            return RowOrientedNullSetter::setNullInstant;
        }

        @Override
        public Consumer<Appender> visit(ArrayType<?, ?> arrayType) {
            return RowOrientedNullSetter::setNullObject;
        }

        @Override
        public Consumer<Appender> visit(CustomType<?> customType) {
            return RowOrientedNullSetter::setNullObject;
        }

        private static void setNullBoolean(Appender appender) {
            BooleanAppender.get(appender).setNull();
        }

        private static void setNullChar(Appender appender) {
            CharAppender.get(appender).setNull();
        }

        private static void setNullByte(Appender appender) {
            ByteAppender.get(appender).setNull();
        }

        private static void setNullShort(Appender appender) {
            ShortAppender.get(appender).setNull();
        }

        private static void setNullInt(Appender appender) {
            IntAppender.get(appender).setNull();
        }

        private static void setNullLong(Appender appender) {
            LongAppender.get(appender).setNull();
        }

        private static void setNullFloat(Appender appender) {
            FloatAppender.get(appender).setNull();
        }

        private static void setNullDouble(Appender appender) {
            DoubleAppender.get(appender).setNull();
        }

        private static void setNullObject(Appender appender) {
            ((ObjectAppender<?>)appender).setNull();
        }

        private static void setNullInstant(Appender appender) {
            InstantAppender.get(appender).setNull();
        }
    }

    private static class ColumnOrientedNullAppender implements Visitor<Consumer<Appender>>, PrimitiveType.Visitor<Consumer<Appender>>, GenericType.Visitor<Consumer<Appender>> {
        private long times;

        private void setTimes(int times) {
            this.times = times;
        }

        @Override
        public Consumer<Appender> visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<Consumer<Appender>>) this);
        }

        @Override
        public Consumer<Appender> visit(GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<Consumer<Appender>>) this);
        }

        @Override
        public Consumer<Appender> visit(BooleanType booleanType) {
            return this::appendNullBooleans;
        }

        @Override
        public Consumer<Appender> visit(ByteType byteType) {
            return this::appendNullBytes;
        }

        @Override
        public Consumer<Appender> visit(CharType charType) {
            return this::appendNullChars;
        }

        @Override
        public Consumer<Appender> visit(ShortType shortType) {
            return this::appendNullShorts;
        }

        @Override
        public Consumer<Appender> visit(IntType intType) {
            return this::appendNullInts;
        }

        @Override
        public Consumer<Appender> visit(LongType longType) {
            return this::appendNullLongs;
        }

        @Override
        public Consumer<Appender> visit(FloatType floatType) {
            return this::appendNullFloats;
        }

        @Override
        public Consumer<Appender> visit(DoubleType doubleType) {
            return this::appendNullDoubles;
        }

        @Override
        public Consumer<Appender> visit(BoxedType<?> boxedType) {
            return this::appendNullObjects;
        }

        @Override
        public Consumer<Appender> visit(StringType stringType) {
            return this::appendNullObjects;
        }

        @Override
        public Consumer<Appender> visit(InstantType instantType) {
            return this::appendNullInstants;
        }

        @Override
        public Consumer<Appender> visit(ArrayType<?, ?> arrayType) {
            return this::appendNullObjects;
        }

        @Override
        public Consumer<Appender> visit(CustomType<?> customType) {
            return this::appendNullObjects;
        }

        private void appendNullBooleans(Appender appender) {
            final BooleanAppender x = BooleanAppender.get(appender);
            for (long i = 0; i < times; i++) {
                BooleanAppender.appendNull(x);
            }
        }

        private void appendNullChars(Appender appender) {
            final CharAppender x = CharAppender.get(appender);
            for (long i = 0; i < times; i++) {
                CharAppender.appendNull(x);
            }
        }

        private void appendNullBytes(Appender appender) {
            final ByteAppender x = ByteAppender.get(appender);
            for (long i = 0; i < times; i++) {
                ByteAppender.appendNull(x);
            }
        }

        private void appendNullShorts(Appender appender) {
            final ShortAppender x = ShortAppender.get(appender);
            for (long i = 0; i < times; i++) {
                ShortAppender.appendNull(x);
            }
        }

        private void appendNullInts(Appender appender) {
            final IntAppender x = IntAppender.get(appender);
            for (long i = 0; i < times; i++) {
                IntAppender.appendNull(x);
            }
        }

        private void appendNullLongs(Appender appender) {
            final LongAppender x = LongAppender.get(appender);
            for (long i = 0; i < times; i++) {
                LongAppender.appendNull(x);
            }
        }

        private void appendNullFloats(Appender appender) {
            final FloatAppender x = FloatAppender.get(appender);
            for (long i = 0; i < times; i++) {
                FloatAppender.appendNull(x);
            }
        }

        private void appendNullDoubles(Appender appender) {
            final DoubleAppender x = DoubleAppender.get(appender);
            for (long i = 0; i < times; i++) {
                DoubleAppender.appendNull(x);
            }
        }

        private void appendNullObjects(Appender appender) {
            final ObjectAppender<?> x = (ObjectAppender<?>)appender;
            for (long i = 0; i < times; i++) {
                ObjectAppender.appendNull(x);
            }
        }

        private void appendNullInstants(Appender appender) {
            final InstantAppender x = InstantAppender.get(appender);
            for (long i = 0; i < times; i++) {
                InstantAppender.appendNull(x);
            }
        }
    }
}
