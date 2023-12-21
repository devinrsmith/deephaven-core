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
import java.util.function.Predicate;

public class What {

    static <V> ObjectProcessor<ConsumerRecord<?, V>> test(
            Predicate<ConsumerRecord<?, V>> predicate,
            ObjectProcessor<ConsumerRecord<?, V>> valueProcessor) {

        final ObjectProcessor<ConsumerRecord<?, ?>> x = ObjectProcessorFunctions.of(List.of(
                ToObjectFunction.of(ConsumerRecordFunctions::topic, Type.stringType()),
                (ToIntFunction<ConsumerRecord<?, ?>>) ConsumerRecordFunctions::partition,
                (ToLongFunction<ConsumerRecord<?, ?>>) ConsumerRecordFunctions::offset,
                (ToIntFunction<ConsumerRecord<?, ?>>) ConsumerRecordFunctions::leaderEpoch));



        final List<ObjectProcessor<? super ConsumerRecord<?, V>>> z = List.of(x, ToObjectFunction.map(null, null));
        final ObjectProcessor<ConsumerRecord<?, V>> ret = ObjectProcessor.combined(z);
        return ret;

        // return ObjectProcessor.<ConsumerRecord<?, V>>combined(List.<ConsumerRecord<?, ? super V>>of(x,
        // valueProcessor));
    }
}
