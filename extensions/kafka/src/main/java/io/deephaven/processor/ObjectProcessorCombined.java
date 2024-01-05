/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class ObjectProcessorCombined<T> implements ObjectProcessor<T> {

    public static <T> ObjectProcessorCombined<T> of(List<ObjectProcessor<? super T>> processors) {
        //noinspection Convert2Diamond
        return new ObjectProcessorCombined<T>(processors.stream()
                .flatMap(ObjectProcessorCombined::destructure)
                .collect(Collectors.toUnmodifiableList()));
    }

    private static <T> Stream<ObjectProcessor<? super T>> destructure(ObjectProcessor<T> processor) {
        return processor instanceof ObjectProcessorCombined
                ? ((ObjectProcessorCombined<T>) processor).processors.stream()
                : Stream.of(processor);
    }


    private final List<ObjectProcessor<? super T>> processors;

    private ObjectProcessorCombined(List<ObjectProcessor<? super T>> processors) {
        this.processors = Objects.requireNonNull(processors);
    }

    @Override
    public int size() {
        return processors.stream().mapToInt(ObjectProcessor::size).sum();
    }

    @Override
    public List<Type<?>> outputTypes() {
        return processors.stream()
                .map(ObjectProcessor::outputTypes)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        int outIx = 0;
        for (ObjectProcessor<? super T> processor : processors) {
            final int toIx = outIx + processor.outputTypes().size();
            processor.processAll(in, out.subList(outIx, toIx));
            outIx = toIx;
        }
    }
}
