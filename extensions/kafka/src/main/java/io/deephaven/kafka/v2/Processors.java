/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.processor.ObjectProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

import java.util.function.Function;

public final class Processors {

    /**
     * Creates a ... Equivalent to {@code ObjectProcessor.map(ConsumerRecord::key, keyProcessor)}.
     *
     * @param keyProcessor the key processor
     * @return the consumer record processor
     * @param <K> the key type
     * @see ConsumerRecord#key()
     * @see ObjectProcessor#map(Function, ObjectProcessor)
     */
    public static <K> ObjectProcessor<ConsumerRecord<K, ?>> key(ObjectProcessor<K> keyProcessor) {
        return ObjectProcessor.map(ConsumerRecord::key, keyProcessor);
    }

    /**
     * Creates a ... Equivalent to {@code ObjectProcessor.map(ConsumerRecord::value, valueProcessor)}.
     *
     * @param valueProcessor the value processor
     * @return the consumer record processor
     * @param <V> the value type
     * @see ConsumerRecord#value()
     * @see ObjectProcessor#map(Function, ObjectProcessor)
     */
    public static <V> ObjectProcessor<ConsumerRecord<?, V>> value(ObjectProcessor<V> valueProcessor) {
        return ObjectProcessor.map(ConsumerRecord::value, valueProcessor);
    }

    /**
     * Creates a ... Equivalent to {@code ObjectProcessor.map(ConsumerRecord::headers, headersProcessor)}.
     *
     * @param headersProcessor the headers processor
     * @return the consumer record processor
     * @see ConsumerRecord#headers()
     * @see ObjectProcessor#map(Function, ObjectProcessor)
     */
    public static ObjectProcessor<ConsumerRecord<?, ?>> headers(ObjectProcessor<Headers> headersProcessor) {
        return ObjectProcessor.map(ConsumerRecord::headers, headersProcessor);
    }

    /**
     * Creates a ... Equivalent to {@code ObjectProcessor.map(record -> ConsumerRecordFunctions.lastHeader(record, key), headerProcessor)}.
     *
     * @param headerProcessor the header processor
     * @return the consumer record processor
     * @see ConsumerRecordFunctions#lastHeader(ConsumerRecord, String)
     * @see ObjectProcessor#map(Function, ObjectProcessor)
     */
    public static ObjectProcessor<ConsumerRecord<?, ?>> lastHeader(String key, ObjectProcessor<byte[]> headerProcessor) {
        return ObjectProcessor.map(record -> ConsumerRecordFunctions.lastHeader(record, key), headerProcessor);
    }
}
