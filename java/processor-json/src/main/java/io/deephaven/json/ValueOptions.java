/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.jackson.ValueProcessor;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Default;

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

public abstract class ValueOptions implements ObjectProcessor.Provider, NamedObjectProcessor.Provider {

    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final Function<List<String>, String> TO_COLUMN_NAME = ValueOptions::toColumnName;

    /**
     * Creates an object processor based on the {@code inputType} with a default {@link JsonFactory}.
     *
     * @param inputType the input type
     * @return the object processor
     * @param <T> the input type
     * @see #stringProcessor(JsonFactory)
     * @see #bytesProcessor(JsonFactory)
     * @see #charsProcessor(JsonFactory)
     * @see #fileProcessor(JsonFactory)
     * @see #urlProcessor(JsonFactory)
     * @see #byteBufferProcessor(JsonFactory)
     */
    @SuppressWarnings("unchecked")
    @Override
    public final <T> ObjectProcessor<? super T> processor(Class<T> inputType) {
        if (String.class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super T>) stringProcessor(JSON_FACTORY);
        }
        if (byte[].class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super T>) bytesProcessor(JSON_FACTORY);
        }
        if (char[].class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super T>) charsProcessor(JSON_FACTORY);
        }
        if (File.class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super T>) fileProcessor(JSON_FACTORY);
        }
        if (URL.class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super T>) urlProcessor(JSON_FACTORY);
        }
        if (ByteBuffer.class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super T>) byteBufferProcessor(JSON_FACTORY);
        }
        throw new IllegalArgumentException("Unable to create JSON processor from type " + inputType.getName());
    }

    /**
     * Creates a named object processor based on the {@code inputType} with a default {@link JsonFactory} and default
     * naming function. Equivalent to
     * {@code NamedObjectProcessor.of(processor(inputType), names(ValueOptions::toColumnName))}.
     *
     * @param inputType the input type
     * @return the named object processor
     * @param <T> the input type
     * @see NamedObjectProcessor#of(ObjectProcessor, Iterable)
     * @see #processor(Class)
     * @see #names(Function)
     * @see #toColumnName(List)
     */
    @Override
    public final <T> NamedObjectProcessor<? super T> named(Class<T> inputType) {
        return NamedObjectProcessor.of(processor(inputType), names(TO_COLUMN_NAME));
    }

    /**
     * Creates a {@link String} json object processor.
     *
     * @param factory the factory
     * @return the object processor
     * @see JsonFactory#createParser(String)
     */
    public final ObjectProcessor<String> stringProcessor(JsonFactory factory) {
        return new StringIn(factory);
    }

    /**
     * Creates a {@code byte[]} json object processor.
     *
     * @param factory the factory
     * @return the object processor
     * @see JsonFactory#createParser(byte[])
     */
    public final ObjectProcessor<byte[]> bytesProcessor(JsonFactory factory) {
        return new BytesIn(factory);
    }

    /**
     * Creates a {@code char[]} json object processor.
     *
     * @param factory the factory
     * @return the object processor
     * @see JsonFactory#createParser(char[])
     */
    public final ObjectProcessor<char[]> charsProcessor(JsonFactory factory) {
        return new CharsIn(factory);
    }

    /**
     * Creates a {@link File} json object processor.
     *
     * @param factory the factory
     * @return the object processor
     * @see JsonFactory#createParser(File)
     */
    public final ObjectProcessor<File> fileProcessor(JsonFactory factory) {
        return new FileIn(factory);
    }

    /**
     * Creates a {@link URL} json object processor.
     *
     * @param factory the factory
     * @return the object processor
     * @see JsonFactory#createParser(URL)
     */
    public final ObjectProcessor<URL> urlProcessor(JsonFactory factory) {
        return new URLIn(factory);
    }

    /**
     * Creates a {@link ByteBuffer} json object processor.
     *
     * @param factory the factory
     * @return the object processor
     */
    public final ObjectProcessor<ByteBuffer> byteBufferProcessor(JsonFactory factory) {
        return new ByteBufferIn(factory);
    }

    public final List<String> names(Function<List<String>, String> f) {
        return paths().map(f).collect(Collectors.toList());
    }

    @Default
    public boolean allowNull() {
        return true;
    }

    @Default
    public boolean allowMissing() {
        return true;
    }

    public final ArrayOptions toArrayOptions() {
        return null;
        // return ArrayOptions.builder()
        // .element(this)
        // .build();
    }

    public abstract <T> T walk(Visitor<T> visitor);

    public interface Visitor<T> {

        T visit(IntOptions _int);

        T visit(LongOptions _long);

        T visit(FloatOptions _float);

        T visit(DoubleOptions _double);

        T visit(ObjectOptions object);

        T visit(InstantOptions instant);

        T visit(InstantNumberOptions instantNumber);

        T visit(BigIntegerOptions bigInteger);

        T visit(BigDecimalOptions bigDecimal);
    }

    public interface Builder<V extends ValueOptions, B extends Builder<V, B>> {

        B allowNull(boolean allowNull);

        B allowMissing(boolean allowMissing);

        V build();
    }


    // todo: what about multivariate?

    abstract int outputCount();

    // todo: is Map<List<String>, Type<?>> easier?
    // or, Stream<(List<String>, Type<?>)>?

    abstract Stream<List<String>> paths();

    abstract Stream<Type<?>> outputTypes();

    abstract ValueProcessor processor(String context, List<WritableChunk<?>> out);

    // for nested / typedescr cases
    ValueOptions withMissingSupport() {
        if (allowMissing()) {
            return this;
        }
        throw new UnsupportedOperationException(); // todo
    }

    final int numColumns() {
        return (int) outputTypes().count();
    }

    static List<String> prefixWith(String prefix, List<String> path) {
        return Stream.concat(Stream.of(prefix), path.stream()).collect(Collectors.toList());
    }

    static Stream<List<String>> prefixWithKeys(Map<String, ? extends ValueOptions> fields) {
        final List<Stream<List<String>>> paths = new ArrayList<>();
        for (Entry<String, ? extends ValueOptions> e : fields.entrySet()) {
            final String key = e.getKey();
            final ValueOptions value = e.getValue();
            final Stream<List<String>> prefixedPaths = value.paths().map(x -> prefixWith(key, x));
            paths.add(prefixedPaths);
        }
        return paths.stream().flatMap(Function.identity());
    }

    public static String toColumnName(List<String> path) {
        // todo: allow user to configure
        return String.join("_", path);
    }

    private class StringIn extends ObjectProcessorJsonValue<String> {
        StringIn(JsonFactory factory) {
            super(factory);
        }

        @Override
        protected JsonParser createParser(JsonFactory factory, String in) throws IOException {
            return factory.createParser(in);
        }
    }

    private class BytesIn extends ObjectProcessorJsonValue<byte[]> {
        BytesIn(JsonFactory factory) {
            super(factory);
        }

        @Override
        protected JsonParser createParser(JsonFactory factory, byte[] in) throws IOException {
            return factory.createParser(in);
        }
    }

    private class ByteBufferIn extends ObjectProcessorJsonValue<ByteBuffer> {
        ByteBufferIn(JsonFactory factory) {
            super(factory);
        }

        @Override
        protected JsonParser createParser(JsonFactory factory, ByteBuffer in) throws IOException {
            if (in.hasArray()) {
                return factory.createParser(in.array(), in.position(), in.remaining());
            }
            return factory.createParser(ByteBufferInputStream.of(in));
        }
    }

    private class CharsIn extends ObjectProcessorJsonValue<char[]> {
        CharsIn(JsonFactory factory) {
            super(factory);
        }

        @Override
        protected JsonParser createParser(JsonFactory factory, char[] in) throws IOException {
            return factory.createParser(in);
        }
    }

    private class FileIn extends ObjectProcessorJsonValue<File> {
        FileIn(JsonFactory factory) {
            super(factory);
        }

        @Override
        protected JsonParser createParser(JsonFactory factory, File in) throws IOException {
            return factory.createParser(in);
        }
    }

    private class URLIn extends ObjectProcessorJsonValue<URL> {
        URLIn(JsonFactory factory) {
            super(factory);
        }

        @Override
        protected JsonParser createParser(JsonFactory factory, URL in) throws IOException {
            return factory.createParser(in);
        }
    }

    private abstract class ObjectProcessorJsonValue<T> implements ObjectProcessor<T> {

        private final JsonFactory factory;

        ObjectProcessorJsonValue(JsonFactory factory) {
            this.factory = Objects.requireNonNull(factory);
        }

        protected abstract JsonParser createParser(JsonFactory factory, T in) throws IOException;

        @Override
        public final int size() {
            return outputCount();
        }

        @Override
        public final List<Type<?>> outputTypes() {
            return ValueOptions.this.outputTypes().collect(Collectors.toList());
        }

        @Override
        public final void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
            try {
                processAllImpl(in, out);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        void processAllImpl(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) throws IOException {
            final ValueProcessor valueProcessor = processor("<root>", out);
            for (int i = 0; i < in.size(); ++i) {
                try (final JsonParser parser = createParser(factory, in.get(i))) {
                    ValueProcessor.processFullJson(valueProcessor, parser);
                }
            }
        }
    }
}
