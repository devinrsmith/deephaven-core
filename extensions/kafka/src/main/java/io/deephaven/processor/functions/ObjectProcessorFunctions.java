/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor.functions;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.functions.TypedFunction;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;

import java.util.List;

public final class ObjectProcessorFunctions {

    /**
     * Creates a function-based processor whose {@link ObjectProcessor#outputTypes()} is the
     * {@link TypedFunction#returnType()} from each function in {@code functions}. The {@code functions} output will be
     * adapted as necessary to ensure compatibility with the required {@link ObjectProcessor#chunkType(Type) chunk
     * types}. The means that callers should <b>not</b> be pre-adapting the function types to match the expected chunk
     * types. For example, a caller with a function of type {@link Type#instantType()} should pass that function in
     * as-is (as opposed to trying to adapt it first into a {@link Type#longType()} function).
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
}
