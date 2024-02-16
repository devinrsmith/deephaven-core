/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.AnyOptions;
import io.deephaven.json.ArrayOptions;
import io.deephaven.json.BigDecimalOptions;
import io.deephaven.json.BigIntegerOptions;
import io.deephaven.json.DoubleOptions;
import io.deephaven.json.FloatOptions;
import io.deephaven.json.InstantNumberOptions;
import io.deephaven.json.InstantOptions;
import io.deephaven.json.IntOptions;
import io.deephaven.json.LocalDateOptions;
import io.deephaven.json.LongOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.SkipOptions;
import io.deephaven.json.StringOptions;
import io.deephaven.json.TupleOptions;
import io.deephaven.json.TypedObjectOptions;
import io.deephaven.json.ValueOptions;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class Mixin implements JacksonProvider {

    // private static final JsonFactory JSON_FACTORY;
    private static final Function<List<String>, String> TO_COLUMN_NAME = Mixin::toColumnName;

    // static {
    // // We'll attach an ObjectMapper if it's on the classpath
    // ObjectCodec objectCodec = null;
    // try {
    // final Class<?> clazz = Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
    // objectCodec = (ObjectCodec) clazz.getDeclaredConstructor().newInstance();
    // } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
    // InvocationTargetException e) {
    // // ignore
    // }
    // JSON_FACTORY = new JsonFactory(objectCodec);
    // }

    static Mixin of(ValueOptions options, JacksonConfiguration config) {
        return options.walk(new MixinImpl(config));
    }

    private final JacksonConfiguration config;

    Mixin(JacksonConfiguration config) {
        this.config = Objects.requireNonNull(config);
    }

    public static String toColumnName(List<String> path) {
        // todo: allow user to configure
        return String.join("_", path);
    }

    final Mixin mixin(ValueOptions options) {
        return of(options, config);
    }

    abstract ValueProcessor processor(String context, List<WritableChunk<?>> out);

    abstract int outputCount();

    // todo: is Map<List<String>, Type<?>> easier?
    // or, Stream<(List<String>, Type<?>)>?
    abstract Stream<List<String>> paths();

    abstract Stream<Type<?>> outputTypes();

    final int numColumns() {
        return (int) outputTypes().count();
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <T> ObjectProcessor<? super T> processor(Class<T> inputType) {
        if (String.class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super T>) stringProcessor();
        }
        if (byte[].class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super T>) bytesProcessor();
        }
        if (char[].class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super T>) charsProcessor();
        }
        if (File.class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super T>) fileProcessor();
        }
        if (URL.class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super T>) urlProcessor();
        }
        if (ByteBuffer.class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super T>) byteBufferProcessor();
        }
        throw new IllegalArgumentException("Unable to create JSON processor from type " + inputType.getName());
    }

    @Override
    public final <T> NamedObjectProcessor<? super T> named(Class<T> inputType) {
        return NamedObjectProcessor.of(processor(inputType), names(TO_COLUMN_NAME));
    }

    @Override
    public final List<String> names(Function<List<String>, String> f) {
        return paths().map(f).collect(Collectors.toList());
    }

    @Override
    public final ObjectProcessor<String> stringProcessor() {
        return new StringIn();
    }

    @Override
    public final ObjectProcessor<byte[]> bytesProcessor() {
        return new BytesIn();
    }

    @Override
    public final ObjectProcessor<char[]> charsProcessor() {
        return new CharsIn();
    }

    @Override
    public final ObjectProcessor<File> fileProcessor() {
        return new FileIn();
    }

    @Override
    public final ObjectProcessor<URL> urlProcessor() {
        return new URLIn();
    }

    @Override
    public final ObjectProcessor<ByteBuffer> byteBufferProcessor() {
        return new ByteBufferIn();
    }

    static List<String> prefixWith(String prefix, List<String> path) {
        return Stream.concat(Stream.of(prefix), path.stream()).collect(Collectors.toList());
    }

    Stream<List<String>> prefixWithKeys(Map<String, ? extends ValueOptions> fields) {
        final List<Stream<List<String>>> paths = new ArrayList<>();
        for (Entry<String, ? extends ValueOptions> e : fields.entrySet()) {
            final String key = e.getKey();
            final ValueOptions value = e.getValue();
            final Stream<List<String>> prefixedPaths = mixin(value).paths().map(x -> prefixWith(key, x));
            paths.add(prefixedPaths);
        }
        return paths.stream().flatMap(Function.identity());
    }

    private class StringIn extends ObjectProcessorJsonValue<String> {
        @Override
        protected JsonParser createParser(JsonFactory factory, String in) throws IOException {
            return factory.createParser(in);
        }
    }

    private class BytesIn extends ObjectProcessorJsonValue<byte[]> {
        @Override
        protected JsonParser createParser(JsonFactory factory, byte[] in) throws IOException {
            return factory.createParser(in);
        }
    }

    private class ByteBufferIn extends ObjectProcessorJsonValue<ByteBuffer> {
        @Override
        protected JsonParser createParser(JsonFactory factory, ByteBuffer in) throws IOException {
            if (in.hasArray()) {
                return factory.createParser(in.array(), in.position(), in.remaining());
            }
            return factory.createParser(ByteBufferInputStream.of(in));
        }
    }

    private class CharsIn extends ObjectProcessorJsonValue<char[]> {
        @Override
        protected JsonParser createParser(JsonFactory factory, char[] in) throws IOException {
            return factory.createParser(in);
        }
    }

    private class FileIn extends ObjectProcessorJsonValue<File> {
        @Override
        protected JsonParser createParser(JsonFactory factory, File in) throws IOException {
            return factory.createParser(in);
        }
    }

    private class URLIn extends ObjectProcessorJsonValue<URL> {
        @Override
        protected JsonParser createParser(JsonFactory factory, URL in) throws IOException {
            return factory.createParser(in);
        }
    }

    private abstract class ObjectProcessorJsonValue<T> implements ObjectProcessor<T> {

        protected abstract JsonParser createParser(JsonFactory factory, T in) throws IOException;

        @Override
        public final int size() {
            return outputCount();
        }

        @Override
        public final List<Type<?>> outputTypes() {
            return Mixin.this.outputTypes().collect(Collectors.toList());
        }

        @Override
        public final void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
            try {
                processAllImpl(in, out);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private JsonParser createParser(T in) throws IOException {
            return createParser(config.factory(), in);
        }

        void processAllImpl(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) throws IOException {
            final ValueProcessor valueProcessor = processor("<root>", out);
            for (int i = 0; i < in.size(); ++i) {
                try (final JsonParser parser = createParser(in.get(i))) {
                    ValueProcessor.processFullJson(valueProcessor, parser);
                }
            }
        }
    }

    private static class MixinImpl implements ValueOptions.Visitor<Mixin> {
        private final JacksonConfiguration config;

        public MixinImpl(JacksonConfiguration config) {
            this.config = Objects.requireNonNull(config);
        }

        @Override
        public Mixin visit(StringOptions _string) {
            return new StringMixin(_string, config);
        }

        @Override
        public Mixin visit(IntOptions _int) {
            return new IntMixin(_int, config);
        }

        @Override
        public Mixin visit(LongOptions _long) {
            return new LongMixin(_long, config);
        }

        @Override
        public Mixin visit(FloatOptions _float) {
            return new FloatMixin(_float, config);
        }

        @Override
        public Mixin visit(DoubleOptions _double) {
            return new DoubleMixin(_double, config);
        }

        @Override
        public Mixin visit(ObjectOptions object) {
            return new ObjectMixin(object, config);
        }

        @Override
        public Mixin visit(InstantOptions instant) {
            return new InstantMixin(instant, config);
        }

        @Override
        public Mixin visit(InstantNumberOptions instantNumber) {
            return new InstantNumberMixin(instantNumber, config);
        }

        @Override
        public Mixin visit(BigIntegerOptions bigInteger) {
            return new BigIntegerMixin(bigInteger, config);
        }

        @Override
        public Mixin visit(BigDecimalOptions bigDecimal) {
            return new BigDecimalMixin(bigDecimal, config);
        }

        @Override
        public Mixin visit(SkipOptions skip) {
            return new SkipMixin(skip, config);
        }

        @Override
        public Mixin visit(TupleOptions tuple) {
            return new TupleMixin(tuple, config);
        }

        @Override
        public Mixin visit(TypedObjectOptions typedObject) {
            return new TypedObjectMixin(typedObject, config);
        }

        @Override
        public Mixin visit(LocalDateOptions localDate) {
            return new LocalDateMixin(localDate, config);
        }

        @Override
        public Mixin visit(ArrayOptions array) {
            return new ArrayMixin(array, config);
        }

        @Override
        public Mixin visit(AnyOptions any) {
            return new AnyMixin(any, config);
        }
    }
}
