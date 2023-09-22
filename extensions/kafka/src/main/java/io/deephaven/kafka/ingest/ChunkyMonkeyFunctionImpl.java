/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.functions.ToBooleanFunction;
import io.deephaven.functions.ToByteFunction;
import io.deephaven.functions.ToCharFunction;
import io.deephaven.functions.ToDoubleFunction;
import io.deephaven.functions.ToFloatFunction;
import io.deephaven.functions.ToIntFunction;
import io.deephaven.functions.ToLongFunction;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.functions.ToPrimitiveFunction;
import io.deephaven.functions.ToShortFunction;
import io.deephaven.functions.TypedFunction;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

final class ChunkyMonkeyFunctionImpl<T> extends ChunkyMonkeyNoLimitBase<T> {

    interface Appender<T> {
        Type<?> returnType();

        void add(WritableChunk<?> dest, T src);

        void append(WritableChunk<?> dest, ObjectChunk<? extends T, ?> src);
    }

    static <T> ChunkyMonkeyFunctionImpl<T> of(List<TypedFunction<T>> functions) {
        return new ChunkyMonkeyFunctionImpl<>(functions.stream().map(AppenderVisitor::of).collect(Collectors.toList()));
    }

    private final List<Appender<? super T>> appenders;

    private ChunkyMonkeyFunctionImpl(List<Appender<? super T>> appenders) {
        this.appenders = Objects.requireNonNull(appenders);
    }

    @Override
    public List<Type<?>> types() {
        return appenders.stream().map(Appender::returnType).collect(Collectors.toList());
    }

    @Override
    public void splay(T in, List<WritableChunk<?>> out) {
        checkChunks(out);
        final int L = appenders.size();
        for (int i = 0; i < L; ++i) {
            appenders.get(i).add(out.get(i), in);
        }
    }

    @Override
    public void splayAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        checkChunks(out);
        final int L = appenders.size();
        for (int i = 0; i < L; ++i) {
            appenders.get(i).append(out.get(i), in);
        }
    }

    private void checkChunks(List<WritableChunk<?>> out) {
        if (appenders.size() != out.size()) {
            throw new IllegalArgumentException();
        }
        // we'll catch mismatched chunk types later when we try to cast them
    }

    private static class AppenderVisitor<T> implements
            TypedFunction.Visitor<T, Appender<T>>,
            ToPrimitiveFunction.Visitor<T, Appender<T>> {
        static <T> Appender<T> of(TypedFunction<T> f) {
            return f.walk(new AppenderVisitor<>());
        }

        @Override
        public Appender<T> visit(ToObjectFunction<T, ?> f) {
            return new ObjectAppender<>(f);
        }

        @Override
        public Appender<T> visit(ToPrimitiveFunction<T> f) {
            return f.walk((ToPrimitiveFunction.Visitor<T, Appender<T>>) this);
        }

        @Override
        public Appender<T> visit(ToBooleanFunction<T> f) {
            return new BooleanAppender<>(f);
        }

        @Override
        public Appender<T> visit(ToCharFunction<T> f) {
            return new CharAppender<>(f);
        }

        @Override
        public Appender<T> visit(ToByteFunction<T> f) {
            return new ByteAppender<>(f);
        }

        @Override
        public Appender<T> visit(ToShortFunction<T> f) {
            return new ShortAppender<>(f);
        }

        @Override
        public Appender<T> visit(ToIntFunction<T> f) {
            return new IntAppender<>(f);
        }

        @Override
        public Appender<T> visit(ToLongFunction<T> f) {
            return new LongAppender<>(f);
        }

        @Override
        public Appender<T> visit(ToFloatFunction<T> f) {
            return new FloatAppender<>(f);
        }

        @Override
        public Appender<T> visit(ToDoubleFunction<T> f) {
            return new DoubleAppender<>(f);
        }
    }

    private static class ObjectAppender<T, R> implements Appender<T> {
        private final ToObjectFunction<? super T, ?> f;

        ObjectAppender(ToObjectFunction<? super T, ?> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public Type<?> returnType() {
            return f.returnType();
        }

        @Override
        public void add(WritableChunk<?> dest, T src) {
            dest.asWritableObjectChunk().add(f.apply(src));
        }

        @Override
        public void append(WritableChunk<?> dest, ObjectChunk<? extends T, ?> src) {
            ChunkUtils.append(dest.asWritableObjectChunk(), f, src);
        }
    }

    private static class BooleanAppender<T> implements Appender<T> {
        private final Predicate<? super T> f;

        BooleanAppender(Predicate<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public Type<?> returnType() {
            return Type.booleanType();
        }

        @Override
        public void add(WritableChunk<?> dest, T src) {
            dest.asWritableBooleanChunk().add(f.test(src));
        }

        @Override
        public void append(WritableChunk<?> dest, ObjectChunk<? extends T, ?> src) {
            ChunkUtils.append(dest.asWritableBooleanChunk(), f, src);
        }
    }

    private static class CharAppender<T> implements Appender<T> {
        private final ToCharFunction<? super T> f;

        CharAppender(ToCharFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public Type<?> returnType() {
            return Type.charType();
        }

        @Override
        public void add(WritableChunk<?> dest, T src) {
            dest.asWritableCharChunk().add(f.applyAsChar(src));
        }

        @Override
        public void append(WritableChunk<?> dest, ObjectChunk<? extends T, ?> src) {
            ChunkUtils.append(dest.asWritableCharChunk(), f, src);
        }
    }

    private static class ByteAppender<T> implements Appender<T> {
        private final ToByteFunction<? super T> f;

        ByteAppender(ToByteFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public Type<?> returnType() {
            return Type.byteType();
        }

        @Override
        public void add(WritableChunk<?> dest, T src) {
            dest.asWritableByteChunk().add(f.applyAsByte(src));
        }

        @Override
        public void append(WritableChunk<?> dest, ObjectChunk<? extends T, ?> src) {
            ChunkUtils.append(dest.asWritableByteChunk(), f, src);
        }
    }

    private static class ShortAppender<T> implements Appender<T> {
        private final ToShortFunction<? super T> f;

        ShortAppender(ToShortFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public Type<?> returnType() {
            return Type.shortType();
        }

        @Override
        public void add(WritableChunk<?> dest, T src) {
            dest.asWritableShortChunk().add(f.applyAsShort(src));
        }

        @Override
        public void append(WritableChunk<?> dest, ObjectChunk<? extends T, ?> src) {
            ChunkUtils.append(dest.asWritableShortChunk(), f, src);
        }
    }

    private static class IntAppender<T> implements Appender<T> {
        private final java.util.function.ToIntFunction<? super T> f;

        IntAppender(java.util.function.ToIntFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public Type<?> returnType() {
            return Type.intType();
        }

        @Override
        public void add(WritableChunk<?> dest, T src) {
            dest.asWritableIntChunk().add(f.applyAsInt(src));
        }

        @Override
        public void append(WritableChunk<?> dest, ObjectChunk<? extends T, ?> src) {
            ChunkUtils.append(dest.asWritableIntChunk(), f, src);
        }
    }

    private static class LongAppender<T> implements Appender<T> {
        private final java.util.function.ToLongFunction<? super T> f;

        LongAppender(java.util.function.ToLongFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public Type<?> returnType() {
            return Type.longType();
        }

        @Override
        public void add(WritableChunk<?> dest, T src) {
            dest.asWritableLongChunk().add(f.applyAsLong(src));
        }

        @Override
        public void append(WritableChunk<?> dest, ObjectChunk<? extends T, ?> src) {
            ChunkUtils.append(dest.asWritableLongChunk(), f, src);
        }
    }

    private static class FloatAppender<T> implements Appender<T> {
        private final ToFloatFunction<? super T> f;

        FloatAppender(ToFloatFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public Type<?> returnType() {
            return Type.floatType();
        }

        @Override
        public void add(WritableChunk<?> dest, T src) {
            dest.asWritableFloatChunk().add(f.applyAsFloat(src));
        }

        @Override
        public void append(WritableChunk<?> dest, ObjectChunk<? extends T, ?> src) {
            ChunkUtils.append(dest.asWritableFloatChunk(), f, src);
        }
    }

    private static class DoubleAppender<T> implements Appender<T> {
        private final java.util.function.ToDoubleFunction<? super T> f;

        DoubleAppender(java.util.function.ToDoubleFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public Type<?> returnType() {
            return Type.doubleType();
        }

        @Override
        public void add(WritableChunk<?> dest, T src) {
            dest.asWritableDoubleChunk().add(f.applyAsDouble(src));
        }

        @Override
        public void append(WritableChunk<?> dest, ObjectChunk<? extends T, ?> src) {
            ChunkUtils.append(dest.asWritableDoubleChunk(), f, src);
        }
    }
}
