/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonFactory;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Default;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class ValueOptions implements ObjectProcessor.Provider, NamedObjectProcessor.Provider {

    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final Function<List<String>, String> TO_COLUMN_NAME = ValueOptions::toColumnName;

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
        throw new IllegalArgumentException("Unable to create JSON processor from type " + inputType.getName());
    }

    @Override
    public final <T> NamedObjectProcessor<? super T> named(Class<T> inputType) {
        return NamedObjectProcessor.of(processor(inputType), names(TO_COLUMN_NAME));
    }

    public final ObjectProcessor<String> stringProcessor(JsonFactory factory) {
        return new ObjectProcessorJsonValue.StringIn(this, factory);
    }

    public final ObjectProcessor<byte[]> bytesProcessor(JsonFactory factory) {
        return new ObjectProcessorJsonValue.BytesIn(this, factory);
    }

    public final ObjectProcessor<char[]> charsProcessor(JsonFactory factory) {
        return new ObjectProcessorJsonValue.CharsIn(this, factory);
    }

    public final ObjectProcessor<File> fileProcessor(JsonFactory factory) {
        return new ObjectProcessorJsonValue.FileIn(this, factory);
    }

    public final ObjectProcessor<URL> urlProcessor(JsonFactory factory) {
        return new ObjectProcessorJsonValue.URLIn(this, factory);
    }

    public final NamedObjectProcessor<String> namedStringProcessor(JsonFactory factory,
            Function<List<String>, String> toColumnName) {
        return NamedObjectProcessor.of(stringProcessor(factory), names(toColumnName));
    }

    public final NamedObjectProcessor<byte[]> namedBytesProcessor(JsonFactory factory,
            Function<List<String>, String> toColumnName) {
        return NamedObjectProcessor.of(bytesProcessor(factory), names(toColumnName));
    }

    public final NamedObjectProcessor<char[]> namedCharsProcessor(JsonFactory factory,
            Function<List<String>, String> toColumnName) {
        return NamedObjectProcessor.of(charsProcessor(factory), names(toColumnName));
    }

    public final NamedObjectProcessor<File> namedFileProcessor(JsonFactory factory,
            Function<List<String>, String> toColumnName) {
        return NamedObjectProcessor.of(fileProcessor(factory), names(toColumnName));
    }

    public final NamedObjectProcessor<URL> namedURLProcessor(JsonFactory factory,
            Function<List<String>, String> toColumnName) {
        return NamedObjectProcessor.of(urlProcessor(factory), names(toColumnName));
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

    public final List<String> names(Function<List<String>, String> f) {
        return paths().map(f).collect(Collectors.toList());
    }

    public interface Builder<V extends ValueOptions, B extends Builder<V, B>> {

        B allowNull(boolean allowNull);

        B allowMissing(boolean allowMissing);

        V build();
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
}
