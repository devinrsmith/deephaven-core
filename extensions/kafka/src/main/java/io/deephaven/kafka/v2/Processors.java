/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

import java.util.function.Function;

public final class Processors {

    /**
     * Creates a {@link NamedObjectProcessor} based on the {@link ConsumerRecordOptions#fields()}.
     *
     * @return the object processor
     * @see ConsumerRecordOptions.Field
     */
    public static <K, V> NamedObjectProcessor<ConsumerRecord<K, V>> basic(ConsumerRecordOptions options) {
        return options.namedProcessor();
    }

    /**
     * Creates a consumer record processor for {@code keyProcessor}. Equivalent to
     * {@code ObjectProcessor.map(ConsumerRecord::key, keyProcessor)}.
     *
     * @param keyProcessor the key processor
     * @return the consumer record processor
     * @param <K> the key type
     * @see ConsumerRecord#key()
     * @see ObjectProcessor#map(Function, ObjectProcessor)
     */
    public static <K, V> ObjectProcessor<ConsumerRecord<K, V>> key(ObjectProcessor<? super K> keyProcessor) {
        return ObjectProcessor.map(ConsumerRecord::key, keyProcessor);
    }

    /**
     * Creates a consumer record processor for {@code valueProcessor}. Equivalent to
     * {@code ObjectProcessor.map(ConsumerRecord::value, valueProcessor)}.
     *
     * @param valueProcessor the value processor
     * @return the consumer record processor
     * @param <V> the value type
     * @see ConsumerRecord#value()
     * @see ObjectProcessor#map(Function, ObjectProcessor)
     */
    public static <K, V> ObjectProcessor<ConsumerRecord<K, V>> value(ObjectProcessor<? super V> valueProcessor) {
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
    public static <K, V> ObjectProcessor<ConsumerRecord<K, V>> headers(ObjectProcessor<Headers> headersProcessor) {
        return ObjectProcessor.map(ConsumerRecord::headers, headersProcessor);
    }

    /**
     * Creates a ... Equivalent to
     * {@code ObjectProcessor.map(record -> ConsumerRecordFunctions.lastHeader(record, key), headerProcessor)}.
     *
     * @param headerProcessor the header processor
     * @return the consumer record processor
     * @see ConsumerRecordFunctions#lastHeader(ConsumerRecord, String)
     * @see ObjectProcessor#map(Function, ObjectProcessor)
     */
    public static <K, V> ObjectProcessor<ConsumerRecord<K, V>> lastHeader(String key,
            ObjectProcessor<byte[]> headerProcessor) {
        return ObjectProcessor.map(record -> ConsumerRecordFunctions.lastHeader(record, key), headerProcessor);
    }


}
