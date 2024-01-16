/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.processor.functions.ObjectProcessorFunctions;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;

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
            return ObjectProcessorFunctions.of(Collections.singletonList(ToObjectFunction.identity(boxedType)));
        }

        @Override
        public ObjectProcessor<T> visit(InstantType instantType) {
            return ObjectProcessorFunctions.of(Collections.singletonList(ToObjectFunction.identity(instantType)));
        }

        @Override
        public ObjectProcessor<T> visit(StringType stringType) {
            // noinspection unchecked
            return (ObjectProcessor<T>) new ObjectProcessorAppendTypedChunk<>(stringType);
        }

        @Override
        public ObjectProcessor<T> visit(ArrayType<?, ?> arrayType) {
            // noinspection unchecked
            return (ObjectProcessor<T>) new ObjectProcessorAppendTypedChunk<>(arrayType);
        }

        @Override
        public ObjectProcessor<T> visit(CustomType<?> customType) {
            // noinspection unchecked
            return (ObjectProcessor<T>) new ObjectProcessorAppendTypedChunk<>(customType);
        }
    }

    private static class ObjectProcessorAppendTypedChunk<T> implements ObjectProcessor<T> {
        private final GenericType<T> inOutType;

        public ObjectProcessorAppendTypedChunk(GenericType<T> inOutType) {
            this.inOutType = Objects.requireNonNull(inOutType);
        }

        @Override
        public List<Type<?>> outputTypes() {
            return Collections.singletonList(inOutType);
        }

        @Override
        public void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {



            final ObjectChunk<T, Any> IN1 = (ObjectChunk<T, Any>) in;
            // final WritableObjectChunk<T, Any> dst = out.get(0).<T>asWritableObjectChunk();
            final WritableObjectChunk<T, Any> dst2 = ((WritableChunk<Any>) out.get(0)).<T>asWritableObjectChunk();

            appendTypedChunk(IN1, dst2);
        }
    }

    private static <T, ATTR extends Any> void appendTypedChunk(ObjectChunk<T, ? extends ATTR> src,
            WritableObjectChunk<T, ATTR> dst) {
        dst.appendTypedChunk(src, 0, src.size());
    }
}
