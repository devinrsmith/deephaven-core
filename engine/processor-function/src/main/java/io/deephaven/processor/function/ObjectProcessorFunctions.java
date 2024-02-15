/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor.function;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.function.ToObjectFunction;
import io.deephaven.function.TypedFunction;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.StringType;

import java.util.List;

public final class ObjectProcessorFunctions {

    /**
     * Creates a function-based processor whose {@link ObjectProcessor#outputTypes()} is the
     * {@link TypedFunction#returnType()} from each function in {@code functions}.
     *
     * <p>
     * The implementation of {@link ObjectProcessor#processAll(ObjectChunk, List)} is column-oriented with a virtual
     * call and cast per-column.
     *
     * @param functions the functions
     * @return the function-based processor
     * @param <T> the object type
     */
    public static <T> ObjectProcessor<T> of(List<TypedFunction<? super T>> functions) {
        return ObjectProcessorFunctionsImpl.create(functions);
    }

    public static <T> ObjectProcessor<T> identity(GenericType<T> type) {
        return type.walk(new IdentityVisitor<>());
    }

    private static final class IdentityVisitor<T> implements GenericType.Visitor<ObjectProcessor<T>> {

        private static <T> ObjectProcessor<T> identityWithTransform(GenericType<T> type) {
            return of(List.of(ToObjectFunction.identity(type)));
        }

        private static <T> ObjectProcessor<T> identityWithSameType(GenericType<T> type) {
            return ObjectProcessor.simple(type);
        }

        @Override
        public ObjectProcessor<T> visit(BoxedType<?> boxedType) {
            // Boxed type gets transformed into primitive type
            // noinspection unchecked
            return identityWithTransform((GenericType<T>) boxedType);
        }

        @Override
        public ObjectProcessor<T> visit(InstantType instantType) {
            // Instant type gets transformed into long nano
            // noinspection unchecked
            return identityWithTransform((GenericType<T>) instantType);
        }

        @Override
        public ObjectProcessor<T> visit(StringType stringType) {
            // noinspection unchecked
            return identityWithSameType((GenericType<T>) stringType);
        }

        @Override
        public ObjectProcessor<T> visit(ArrayType<?, ?> arrayType) {
            // noinspection unchecked
            return identityWithSameType((GenericType<T>) arrayType);
        }

        @Override
        public ObjectProcessor<T> visit(CustomType<?> customType) {
            // noinspection unchecked
            return identityWithSameType((GenericType<T>) customType);
        }
    }
}
