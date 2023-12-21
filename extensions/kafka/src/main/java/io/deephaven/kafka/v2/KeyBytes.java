/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public class KeyBytes implements ObjectProcessor<ConsumerRecord<?, ?>> {
    @Override
    public List<Type<?>> outputTypes() {
        return List.of(Type.intType());
    }

    @Override
    public void processAll(ObjectChunk<? extends ConsumerRecord<?, ?>, ?> in, List<WritableChunk<?>> out) {


    }
}
