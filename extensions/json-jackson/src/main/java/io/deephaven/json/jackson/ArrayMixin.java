/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ArrayOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.ValueOptions;
import io.deephaven.json.jackson.DoubleValueProcessor.ToDouble;
import io.deephaven.json.jackson.IntValueProcessor.ToInt;
import io.deephaven.json.jackson.LongValueProcessor.ToLong;
import io.deephaven.json.jackson.ObjectValueProcessor.ToObject;
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class ArrayMixin extends Mixin<ArrayOptions> {

    public ArrayMixin(ArrayOptions options, JsonFactory factory) {
        super(factory, options);
    }

    final boolean asArray() {
        // todo, expose to user, provide option to use ChunkProvider (ie, multiple rows)
        return true;
    }

    Mixin<?> element() {
        return mixin(options.element());
    }

    @Override
    public int outputCount() {
        return element().outputCount();
    }

    @Override
    public Stream<List<String>> paths() {
        return element().paths();
    }

    @Override
    public Stream<Type<?>> outputTypes() {
//        if (element().numColumns() != 1) {
//            throw new IllegalArgumentException("Need multivariate (ChunkProvider) support for this");
//        }
        if (!asArray()) {
            throw new IllegalArgumentException("Need multivariate (ChunkProvider) support for this");
        }
        return element().outputTypes().map(Type::arrayType);
        //return element().outputTypes();
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        if (options.element() instanceof ObjectOptions) {
            final ObjectOptions elementOptions = (ObjectOptions) options.element();
            final Map<String, ValueProcessor> processors = new LinkedHashMap<>(elementOptions.fields().size());
            int ix = 0;
            for (Entry<String, ValueOptions> e : elementOptions.fields().entrySet()) {
                final Mixin<?> mixin = mixin(e.getValue());
                final int numColumns = mixin.numColumns();


                final List<WritableChunk<?>> sublist = out.subList(ix, ix + numColumns);
                //mixin.processor();


                processors.put(e.getKey(), mixin.processor(context, sublist));



                ix += numColumns;
            }
            // todo: need array banding around this.
            final ValueProcessor impls = ((ObjectMixin) element()).processorImpl(processors);
            return new ArrayValueProcessor(impls);
        }
        if (element().numColumns() != 1) {
            throw new IllegalArgumentException("Need multivariate (ChunkProvider) support for this");
        }
        // todo: we should be abel to work w/ ObjectOptions and do arrays for each individual type
        if (!asArray()) {
            throw new IllegalArgumentException("Need multivariate (ChunkProvider) support for this");
        }
        return Objects.requireNonNull(element().outputTypes().findFirst().orElseThrow().walk(new ToProcessor(context, out)));
    }


    private class ToProcessor implements Type.Visitor<ValueProcessor>, PrimitiveType.Visitor<ValueProcessor>,
            GenericType.Visitor<ValueProcessor> {
        private final String context;
        private final List<WritableChunk<?>> out;

        ToProcessor(String context, List<WritableChunk<?>> out) {
            this.context = Objects.requireNonNull(context);
            this.out = Objects.requireNonNull(out);
        }

        @Override
        public ValueProcessor visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<ValueProcessor>) this);
        }

        @Override
        public ValueProcessor visit(GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<ValueProcessor>) this);
        }

        @Override
        public ValueProcessor visit(BooleanType booleanType) {
            return null;
        }

        @Override
        public ValueProcessor visit(ByteType byteType) {
            return null;
        }

        @Override
        public ValueProcessor visit(CharType charType) {
            return null;
        }

        @Override
        public ValueProcessor visit(ShortType shortType) {
            return null;
        }

        @Override
        public ValueProcessor visit(IntType intType) {

            return new ArrayValueProcessor(element().processor(context, out));

            return new ObjectValueProcessor<>(out.get(0).asWritableObjectChunk(), new ToIntArrayImpl(((IntMixin)element()).intImpl()));
        }

        @Override
        public ValueProcessor visit(LongType longType) {
            return new ObjectValueProcessor<>(out.get(0).asWritableObjectChunk(), new ToLongArrayImpl(((LongMixin)element()).longImpl()));
        }

        @Override
        public ValueProcessor visit(FloatType floatType) {
            return null;
        }

        @Override
        public ValueProcessor visit(DoubleType doubleType) {
            return new ObjectValueProcessor<>(out.get(0).asWritableObjectChunk(), new ToDoubleArrayImpl(((DoubleMixin)element()).doubleImpl()));
        }

        @Override
        public ValueProcessor visit(BoxedType<?> boxedType) {
            return null;
        }

        @Override
        public ValueProcessor visit(StringType stringType) {
            return null;
        }

        @Override
        public ValueProcessor visit(InstantType instantType) {
            return null;
        }

        @Override
        public ValueProcessor visit(ArrayType<?, ?> arrayType) {
            return null;
        }

        @Override
        public ValueProcessor visit(CustomType<?> customType) {
            return null;
        }
    }

    class ToIntArrayImpl implements ToObject<int[]> {
        private final ToInt toInt;

        public ToIntArrayImpl(ToInt toInt) {
            this.toInt = Objects.requireNonNull(toInt);
        }

        @Override
        public int[] parseValue(JsonParser parser) throws IOException {
            if (options.allowNull() && parser.hasToken(JsonToken.VALUE_NULL)) {
                return null;
            }
            Helpers.assertCurrentToken(parser, JsonToken.START_ARRAY);
            parser.nextToken();
            try {
                return toIntArray(parser);
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }

        private int[] toIntArray(JsonParser parser) {
            return StreamSupport.intStream(Spliterators.spliteratorUnknownSize(new IntIter(parser, toInt),
                    Spliterator.IMMUTABLE | Spliterator.ORDERED), false).toArray();
        }

        @Override
        public int[] parseMissing(JsonParser parser) throws IOException {
            if (!options.allowMissing()) {
                throw Helpers.mismatchMissing(parser, int[].class);
            }
            return null;
        }
    }

    class ToLongArrayImpl implements ToObject<long[]> {
        private final ToLong toInt;

        public ToLongArrayImpl(ToLong toLong) {
            this.toInt = Objects.requireNonNull(toLong);
        }

        @Override
        public long[] parseValue(JsonParser parser) throws IOException {
            if (options.allowNull() && parser.hasToken(JsonToken.VALUE_NULL)) {
                return null;
            }
            Helpers.assertCurrentToken(parser, JsonToken.START_ARRAY);
            parser.nextToken();
            try {
                return toLongArray(parser);
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }

        private long[] toLongArray(JsonParser parser) {
            return StreamSupport.longStream(Spliterators.spliteratorUnknownSize(new LongIter(parser, toInt),
                    Spliterator.IMMUTABLE | Spliterator.ORDERED), false).toArray();
        }

        @Override
        public long[] parseMissing(JsonParser parser) throws IOException {
            if (!options.allowMissing()) {
                throw Helpers.mismatchMissing(parser, long[].class);
            }
            return null;
        }
    }

    class ToDoubleArrayImpl implements ToObject<double[]> {
        private final ToDouble toInt;

        public ToDoubleArrayImpl(ToDouble toDouble) {
            this.toInt = Objects.requireNonNull(toDouble);
        }

        @Override
        public double[] parseValue(JsonParser parser) throws IOException {
            if (options.allowNull() && parser.hasToken(JsonToken.VALUE_NULL)) {
                return null;
            }
            Helpers.assertCurrentToken(parser, JsonToken.START_ARRAY);
            parser.nextToken();
            try {
                return toDoubleArray(parser);
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }

        private double[] toDoubleArray(JsonParser parser) {
            return StreamSupport.doubleStream(Spliterators.spliteratorUnknownSize(new DoubleIter(parser, toInt),
                    Spliterator.IMMUTABLE | Spliterator.ORDERED), false).toArray();
        }

        @Override
        public double[] parseMissing(JsonParser parser) throws IOException {
            if (!options.allowMissing()) {
                throw Helpers.mismatchMissing(parser, double[].class);
            }
            return null;
        }
    }

    private static class IntIter implements PrimitiveIterator.OfInt {
        private final JsonParser parser;
        private final ToInt toInt;

        public IntIter(JsonParser parser, ToInt toInt) {
            this.parser = Objects.requireNonNull(parser);
            this.toInt = Objects.requireNonNull(toInt);
        }

        @Override
        public int nextInt() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                return nextIntImpl();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private int nextIntImpl() throws IOException {
            final int x = toInt.parseValue(parser);
            parser.nextToken();
            return x;
        }

        @Override
        public boolean hasNext() {
            return !parser.hasToken(JsonToken.END_ARRAY);
        }
    }

    private static class LongIter implements PrimitiveIterator.OfLong {
        private final JsonParser parser;
        private final ToLong toLong;

        public LongIter(JsonParser parser, ToLong toLong) {
            this.parser = Objects.requireNonNull(parser);
            this.toLong = Objects.requireNonNull(toLong);
        }

        @Override
        public long nextLong() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                return nextLongImpl();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private long nextLongImpl() throws IOException {
            final long x = toLong.parseValue(parser);
            parser.nextToken();
            return x;
        }

        @Override
        public boolean hasNext() {
            return !parser.hasToken(JsonToken.END_ARRAY);
        }
    }

    private static class DoubleIter implements PrimitiveIterator.OfDouble {
        private final JsonParser parser;
        private final ToDouble toDouble;

        public DoubleIter(JsonParser parser, ToDouble toDouble) {
            this.parser = Objects.requireNonNull(parser);
            this.toDouble = Objects.requireNonNull(toDouble);
        }

        @Override
        public double nextDouble() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                return nextDoubleImpl();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private double nextDoubleImpl() throws IOException {
            final double x = toDouble.parseValue(parser);
            parser.nextToken();
            return x;
        }

        @Override
        public boolean hasNext() {
            return !parser.hasToken(JsonToken.END_ARRAY);
        }
    }
}
