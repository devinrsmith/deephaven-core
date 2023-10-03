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
import io.deephaven.qst.type.GenericType.Visitor;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.type.TypeUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

final class ChunkyMonkeyFunctionImpl<T> extends ChunkyMonkeyNoLimitBase<T> {

    interface Appender<T> {

        void add(WritableChunk<?> dest, T src);

        void append(WritableChunk<?> dest, ObjectChunk<? extends T, ?> src);
    }

    static <T> ChunkyMonkeyFunctionImpl<T> of(List<TypedFunction<T>> functions) {
        final List<Type<?>> logicalTypes = functions.stream()
                .map(TypedFunction::returnType)
                .collect(Collectors.toUnmodifiableList());
        final List<Appender<? super T>> appenders = functions.stream()
                .map(AppenderVisitor::of)
                .collect(Collectors.toList());
        return new ChunkyMonkeyFunctionImpl<>(logicalTypes, appenders);
    }

    private final List<Type<?>> logicalTypes;
    private final List<Appender<? super T>> appenders;

    private ChunkyMonkeyFunctionImpl(List<Type<?>> logicalTypes, List<Appender<? super T>> appenders) {
        this.logicalTypes = Objects.requireNonNull(logicalTypes);
        this.appenders = Objects.requireNonNull(appenders);
    }

    @Override
    public List<Type<?>> outputTypes() {
        return logicalTypes;
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
        public Appender<T> visit(ToPrimitiveFunction<T> f) {
            return f.walk((ToPrimitiveFunction.Visitor<T, Appender<T>>) this);
        }

        @Override
        public Appender<T> visit(ToBooleanFunction<T> f) {
            return ByteAppender.from(f);
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

        @Override
        public Appender<T> visit(ToObjectFunction<T, ?> f) {
            return f.returnType().walk(new Visitor<>() {
                @Override
                public Appender<T> visit(BoxedType<?> boxedType) {
                    // todo: we should have common unbox impls that handle this.
                    return boxedType.walk(new BoxedType.Visitor<>() {
                        @Override
                        public Appender<T> visit(BoxedBooleanType booleanType) {
                            return ByteAppender.from(f.cast(booleanType));
                        }

                        @Override
                        public Appender<T> visit(BoxedByteType byteType) {
                            return AppenderVisitor.this.visit(f.cast(byteType).mapToByte(TypeUtils::unbox));
                        }

                        @Override
                        public Appender<T> visit(BoxedCharType charType) {
                            return AppenderVisitor.this.visit(f.cast(charType).mapToChar(TypeUtils::unbox));
                        }

                        @Override
                        public Appender<T> visit(BoxedShortType shortType) {
                            return AppenderVisitor.this.visit(f.cast(shortType).mapToShort(TypeUtils::unbox));
                        }

                        @Override
                        public Appender<T> visit(BoxedIntType intType) {
                            return AppenderVisitor.this.visit(f.cast(intType).mapToInt(TypeUtils::unbox));
                        }

                        @Override
                        public Appender<T> visit(BoxedLongType longType) {
                            return AppenderVisitor.this.visit(f.cast(longType).mapToLong(TypeUtils::unbox));
                        }

                        @Override
                        public Appender<T> visit(BoxedFloatType floatType) {
                            return AppenderVisitor.this.visit(f.cast(floatType).mapToFloat(TypeUtils::unbox));
                        }

                        @Override
                        public Appender<T> visit(BoxedDoubleType doubleType) {
                            return AppenderVisitor.this.visit(f.cast(doubleType).mapToDouble(TypeUtils::unbox));
                        }
                    });
                }

                @Override
                public Appender<T> visit(StringType stringType) {
                    return new ObjectAppender<>(f);
                }

                @Override
                public Appender<T> visit(InstantType instantType) {
                    // to long function
                    return AppenderVisitor.this.visit(f.cast(instantType).mapToLong(DateTimeUtils::epochNanos));
                }

                @Override
                public Appender<T> visit(ArrayType<?, ?> arrayType) {
                    return new ObjectAppender<>(f);
                }

                @Override
                public Appender<T> visit(CustomType<?> customType) {
                    return new ObjectAppender<>(f);
                }
            });
        }
    }

    private static class ObjectAppender<T> implements Appender<T> {
        private final ToObjectFunction<? super T, ?> f;

        ObjectAppender(ToObjectFunction<? super T, ?> f) {
            this.f = Objects.requireNonNull(f);
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

    // private static class BooleanAppender<T> implements Appender<T> {
    // private final Predicate<? super T> f;
    //
    // BooleanAppender(Predicate<? super T> f) {
    // this.f = Objects.requireNonNull(f);
    // }
    //
    // @Override
    // public Type<?> returnType() {
    // return Type.booleanType();
    // }
    //
    // @Override
    // public void add(WritableChunk<?> dest, T src) {
    // dest.asWritableBooleanChunk().add(f.test(src));
    // }
    //
    // @Override
    // public void append(WritableChunk<?> dest, ObjectChunk<? extends T, ?> src) {
    // ChunkUtils.append(dest.asWritableBooleanChunk(), f, src);
    // }
    // }

    private static class CharAppender<T> implements Appender<T> {
        private final ToCharFunction<? super T> f;

        CharAppender(ToCharFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
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

        static <T> ByteAppender<T> from(ToBooleanFunction<? super T> f) {
            return new ByteAppender<>(x -> BooleanUtils.booleanAsByte(f.test(x)));
        }

        static <T> ByteAppender<T> from(ToObjectFunction<? super T, ? extends Boolean> f) {
            return new ByteAppender<>(f.mapToByte(BooleanUtils::booleanAsByte));
        }

        private final ToByteFunction<? super T> f;


        ByteAppender(ToByteFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
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
        public void add(WritableChunk<?> dest, T src) {
            dest.asWritableDoubleChunk().add(f.applyAsDouble(src));
        }

        @Override
        public void append(WritableChunk<?> dest, ObjectChunk<? extends T, ?> src) {
            ChunkUtils.append(dest.asWritableDoubleChunk(), f, src);
        }
    }
}
