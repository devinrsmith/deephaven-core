//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

final class MixinJacksonProvider implements JacksonProvider {
    private final Mixin<?> mixin;
    private final JsonFactory factory;

    MixinJacksonProvider(Mixin<?> mixin, JsonFactory factory) {
        this.mixin = Objects.requireNonNull(mixin);
        this.factory = Objects.requireNonNull(factory);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <X> ObjectProcessor<? super X> processor(Type<X> inputType) {
        final Class<X> clazz = inputType.clazz();
        if (String.class.isAssignableFrom(clazz)) {
            return (ObjectProcessor<? super X>) stringProcessor();
        }
        if (byte[].class.isAssignableFrom(clazz)) {
            return (ObjectProcessor<? super X>) bytesProcessor();
        }
        if (char[].class.isAssignableFrom(clazz)) {
            return (ObjectProcessor<? super X>) charsProcessor();
        }
        if (File.class.isAssignableFrom(clazz)) {
            return (ObjectProcessor<? super X>) fileProcessor();
        }
        if (Path.class.isAssignableFrom(clazz)) {
            return (ObjectProcessor<? super X>) pathProcessor();
        }
        if (URL.class.isAssignableFrom(clazz)) {
            return (ObjectProcessor<? super X>) urlProcessor();
        }
        if (ByteBuffer.class.isAssignableFrom(clazz)) {
            return (ObjectProcessor<? super X>) byteBufferProcessor();
        }
        if (CharBuffer.class.isAssignableFrom(clazz)) {
            return (ObjectProcessor<? super X>) charBufferProcessor();
        }
        throw new IllegalArgumentException("Unable to create JSON processor from type " + inputType);
    }

    @Override
    public List<Type<?>> outputTypes() {
        return mixin.outputTypes();
    }

    @Override
    public List<String> names() {
        return mixin.names();
    }

    @Override
    public List<String> names(Function<List<String>, String> f) {
        return mixin.names(f);
    }

    @Override
    public int outputSize() {
        return mixin.outputSize();
    }

    @Override
    public ObjectProcessor<String> stringProcessor() {
        return new StringIn();
    }

    @Override
    public ObjectProcessor<byte[]> bytesProcessor() {
        return new BytesIn();
    }

    @Override
    public ObjectProcessor<char[]> charsProcessor() {
        return new CharsIn();
    }

    @Override
    public ObjectProcessor<File> fileProcessor() {
        return new FileIn();
    }

    @Override
    public ObjectProcessor<Path> pathProcessor() {
        return new PathIn();
    }

    @Override
    public ObjectProcessor<URL> urlProcessor() {
        return new URLIn();
    }

    @Override
    public ObjectProcessor<ByteBuffer> byteBufferProcessor() {
        return new ByteBufferIn();
    }

    @Override
    public ObjectProcessor<CharBuffer> charBufferProcessor() {
        return new CharBufferIn();
    }

    private abstract class ObjectProcessorMixin<X> extends ObjectProcessorJsonValue<X> {
        public ObjectProcessorMixin() {
            super(mixin.processor("<root>"));
        }
    }

    private class StringIn extends ObjectProcessorMixin<String> {
        @Override
        protected JsonParser createParser(String in) throws IOException {
            return JacksonSource.of(factory, in);
        }
    }

    private class BytesIn extends ObjectProcessorMixin<byte[]> {
        @Override
        protected JsonParser createParser(byte[] in) throws IOException {
            return JacksonSource.of(factory, in, 0, in.length);
        }
    }

    private class ByteBufferIn extends ObjectProcessorMixin<ByteBuffer> {
        @Override
        protected JsonParser createParser(ByteBuffer in) throws IOException {
            return JacksonSource.of(factory, in);
        }
    }

    private class CharBufferIn extends ObjectProcessorMixin<CharBuffer> {
        @Override
        protected JsonParser createParser(CharBuffer in) throws IOException {
            return JacksonSource.of(factory, in);
        }
    }

    private class CharsIn extends ObjectProcessorMixin<char[]> {
        @Override
        protected JsonParser createParser(char[] in) throws IOException {
            return JacksonSource.of(factory, in, 0, in.length);
        }
    }

    private class FileIn extends ObjectProcessorMixin<File> {
        @Override
        protected JsonParser createParser(File in) throws IOException {
            return JacksonSource.of(factory, in);
        }
    }

    private class PathIn extends ObjectProcessorMixin<Path> {
        @Override
        protected JsonParser createParser(Path in) throws IOException {
            return JacksonSource.of(factory, in);
        }
    }

    private class URLIn extends ObjectProcessorMixin<URL> {
        @Override
        protected JsonParser createParser(URL in) throws IOException {
            return JacksonSource.of(factory, in);
        }
    }
}
