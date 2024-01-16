/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

final class ObjectProcessorSimple {

    public static <T> ObjectProcessor<T> of(GenericType<T> inType) {
        return inType.walk(new V<>());
    }

    private static class V<T> implements GenericType.Visitor<ObjectProcessor<T>> {
        @Override
        public ObjectProcessor<T> visit(BoxedType<?> boxedType) {
            return null;
        }

        @Override
        public ObjectProcessor<T> visit(StringType stringType) {
            //noinspection unchecked
            return (ObjectProcessor<T>) new ObjectProcessorCopy<>(stringType);
        }

        @Override
        public ObjectProcessor<T> visit(InstantType instantType) {
            return null;
        }

        @Override
        public ObjectProcessor<T> visit(ArrayType<?, ?> arrayType) {
            return null;
        }

        @Override
        public ObjectProcessor<T> visit(CustomType<?> customType) {
            return null;
        }
    }

    enum ObjectProcessorInteger implements ObjectProcessor<Integer> {
        INSTANCE;

        private static int get(ObjectChunk<? extends Integer, ?> in, int pos) {
            final Integer x = in.get(pos);
            // noinspection RedundantCast
            return x == null ? QueryConstants.NULL_INT : (int) x;
        }

        @Override
        public List<Type<?>> outputTypes() {
            return Collections.singletonList(Type.intType());
        }

        @Override
        public void processAll(ObjectChunk<? extends Integer, ?> in, List<WritableChunk<?>> out) {
            // noinspection resource
            final WritableIntChunk<?> outChunk = out.get(0).asWritableIntChunk();
            final int outPos = outChunk.size();
            final int size = in.size();
            for (int i = 0; i < size; ++i) {
                outChunk.set(outPos + i, get(in, i));
            }
            outChunk.setSize(outPos + size);
        }
    }

    enum ObjectProcessorLong implements ObjectProcessor<Long> {
        INSTANCE;

        private static long get(ObjectChunk<? extends Long, ?> in, int pos) {
            final Long x = in.get(pos);
            // noinspection RedundantCast
            return x == null ? QueryConstants.NULL_LONG : (long) x;
        }

        @Override
        public List<Type<?>> outputTypes() {
            return Collections.singletonList(Type.intType());
        }

        @Override
        public void processAll(ObjectChunk<? extends Long, ?> in, List<WritableChunk<?>> out) {
            // noinspection resource
            final WritableLongChunk<?> outChunk = out.get(0).asWritableLongChunk();
            final int outPos = outChunk.size();
            final int size = in.size();
            for (int i = 0; i < size; ++i) {
                outChunk.set(outPos + i, get(in, i));
            }
            outChunk.setSize(outPos + size);
        }
    }

    private static class ObjectProcessorCopy<T> implements ObjectProcessor<T> {

        private final GenericType<T> inOutType;

        public ObjectProcessorCopy(GenericType<T> inOutType) {
            this.inOutType = Objects.requireNonNull(inOutType);
        }

        @Override
        public List<Type<?>> outputTypes() {
            return Collections.singletonList(inOutType);
        }

        @Override
        public void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {



            final ObjectChunk<T, Any> IN1 = (ObjectChunk<T, Any>) in;
//            final WritableObjectChunk<T, Any> dst = out.get(0).<T>asWritableObjectChunk();
            final WritableObjectChunk<T, Any> dst2 = ((WritableChunk<Any>) out.get(0)).<T>asWritableObjectChunk();

            appendTypedChunk(IN1, dst2);
        }
    }

    private static <T, ATTR extends Any> void appendTypedChunk(ObjectChunk<T, ? extends ATTR> src, WritableObjectChunk<T, ATTR> dst) {
        dst.appendTypedChunk(src, 0, src.size());
    }
}
