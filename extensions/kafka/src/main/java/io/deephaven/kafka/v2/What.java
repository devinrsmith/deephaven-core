/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.functions.ToIntFunction;
import io.deephaven.functions.ToLongFunction;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.processor.functions.ObjectProcessorFunctions;
import io.deephaven.qst.type.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public class What {

    static <V> ObjectProcessor<ConsumerRecord<?, V>> test(ObjectProcessor<V> valueProcessor) {

        final ObjectProcessor<ConsumerRecord<?, ?>> common = ObjectProcessorFunctions.of(List.of(
                ToObjectFunction.of(ConsumerRecordFunctions::topic, Type.stringType()),
                (ToIntFunction<ConsumerRecord<?, ?>>) ConsumerRecordFunctions::partition,
                (ToLongFunction<ConsumerRecord<?, ?>>) ConsumerRecordFunctions::offset,
                (ToIntFunction<ConsumerRecord<?, ?>>) ConsumerRecordFunctions::leaderEpoch));

        final ObjectProcessor<ConsumerRecord<?, V>> crv = ObjectProcessor.map(ConsumerRecordFunctions::value, valueProcessor);

        return ObjectProcessor.combined(List.of(common, crv));
    }
}
