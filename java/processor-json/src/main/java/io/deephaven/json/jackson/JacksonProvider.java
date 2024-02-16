/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import io.deephaven.json.ValueOptions;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;

import java.io.File;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

public interface JacksonProvider extends NamedObjectProcessor.Provider, ObjectProcessor.Provider {

    /**
     * Creates a jackson provider using the {@link JacksonConfiguration#defaultInstance() default configuration}.
     * Equivalent to {@code of(options, JacksonConfiguration.defaultInstance())}.
     *
     * @param options the object options
     * @return the jackson provider
     */
    static JacksonProvider of(ValueOptions options) {
        return of(options, JacksonConfiguration.defaultInstance());
    }

    /**
     * Creates a jackson provider using the provided {@code config}.
     *
     * @param options the object options
     * @param config the jackson configuration
     * @return the jackson provider
     */
    static JacksonProvider of(ValueOptions options, JacksonConfiguration config) {
        return Mixin.of(options, config);
    }

    /**
     * Creates an object processor based on the {@code inputType} with a default {@link JsonFactory}.
     *
     * @param inputType the input type
     * @return the object processor
     * @param <T> the input type
     * @see #stringProcessor()
     * @see #bytesProcessor()
     * @see #charsProcessor()
     * @see #fileProcessor()
     * @see #urlProcessor()
     * @see #byteBufferProcessor()
     */
    @Override
    <T> ObjectProcessor<? super T> processor(Class<T> inputType);

    /**
     * Creates a named object processor based on the {@code inputType} with a default {@link JsonFactory} and default
     * naming function. Equivalent to
     * {@code NamedObjectProcessor.of(processor(inputType), names(JacksonProcessor::toColumnName))}.
     *
     * @param inputType the input type
     * @return the named object processor
     * @param <T> the input type
     * @see NamedObjectProcessor#of(ObjectProcessor, Iterable)
     * @see #processor(Class)
     * @see #names(Function)
     * @see Mixin#toColumnName(List)
     */
    @Override
    <T> NamedObjectProcessor<? super T> named(Class<T> inputType);

    List<String> names(Function<List<String>, String> f);

    /**
     * Creates a {@link String} json object processor.
     *
     * @return the object processor
     * @see JsonFactory#createParser(String)
     */
    ObjectProcessor<String> stringProcessor();

    /**
     * Creates a {@code byte[]} json object processor.
     *
     * @return the object processor
     * @see JsonFactory#createParser(byte[])
     */
    ObjectProcessor<byte[]> bytesProcessor();

    /**
     * Creates a {@code char[]} json object processor.
     *
     * @return the object processor
     * @see JsonFactory#createParser(char[])
     */
    ObjectProcessor<char[]> charsProcessor();

    /**
     * Creates a {@link File} json object processor.
     *
     * @return the object processor
     * @see JsonFactory#createParser(File)
     */
    ObjectProcessor<File> fileProcessor();

    /**
     * Creates a {@link URL} json object processor.
     *
     * @return the object processor
     * @see JsonFactory#createParser(URL)
     */
    ObjectProcessor<URL> urlProcessor();

    /**
     * Creates a {@link ByteBuffer} json object processor.
     *
     * @return the object processor
     */
    ObjectProcessor<ByteBuffer> byteBufferProcessor();
}
