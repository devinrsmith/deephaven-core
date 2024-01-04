/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.functions.ToObjectFunction;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.processor.functions.ObjectProcessorFunctions;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

import java.util.List;
import java.util.function.Function;

public final class Processors {

    // /**
    // *
    // * @return
    // * @param <K>
    // * @param <V>
    // */
    // public static <K, V> ObjectProcessor<ConsumerRecord<K, V>> basic() {
    // return basic(ConsumerRecordOptions.of());
    // }

    /**
     * Creates an {@link ObjectProcessor} that contains the following output types and logic:
     *
     * <table>
     * <tr>
     * <th>If not {@code null}</th>
     * <th>Type</th>
     * <th>Logic</th>
     * </tr>
     * <tr>
     * <th>{@link ConsumerRecordOptions#topic()}</th>
     * <th>{@link Type#stringType()}</th>
     * <th>{@link ConsumerRecord#topic()}</th>
     * </tr>
     * <tr>
     * <th>{@link ConsumerRecordOptions#partition()}</th>
     * <th>{@link Type#intType()}</th>
     * <th>{@link ConsumerRecord#partition()}</th>
     * </tr>
     * <tr>
     * <th>{@link ConsumerRecordOptions#offset()}</th>
     * <th>{@link Type#longType()}</th>
     * <th>{@link ConsumerRecord#offset()}</th>
     * </tr>
     * <tr>
     * <th>{@link ConsumerRecordOptions#leaderEpoch()}</th>
     * <th>{@link Type#intType()}</th>
     * <th>{@link ConsumerRecordFunctions#leaderEpoch(ConsumerRecord)}</th>
     * </tr>
     * <tr>
     * <th>{@link ConsumerRecordOptions#timestampType()}</th>
     * <th>{@link Type#ofCustom(Class) Type.ofCustom(TimestampType.class)}</th>
     * <th>{@link ConsumerRecord#timestampType()}</th>
     * </tr>
     * <tr>
     * <th>{@link ConsumerRecordOptions#timestamp()}</th>
     * <th>{@link Type#instantType()}</th>
     * <th>{@link ConsumerRecordFunctions#timestampEpochNanos(ConsumerRecord)}</th>
     * </tr>
     * <tr>
     * <th>{@link ConsumerRecordOptions#serializedKeySize()}</th>
     * <th>{@link Type#intType()}</th>
     * <th>{@link ConsumerRecordFunctions#serializedKeySize(ConsumerRecord)}</th>
     * </tr>
     * <tr>
     * <th>{@link ConsumerRecordOptions#serializedValueSize()}</th>
     * <th>{@link Type#intType()}</th>
     * <th>{@link ConsumerRecordFunctions#serializedValueSize(ConsumerRecord)}</th>
     * </tr>
     * </table>
     *
     * @return the object processor
     */
    public static <K, V> ObjectProcessor<ConsumerRecord<K, V>> basic(ConsumerRecordOptions options) {
        return options.processor();
    }

    public static <K, V> ObjectProcessor<ConsumerRecord<K, V>> key(GenericType<K> keyType) {
        return ObjectProcessorFunctions.of(List.of(ToObjectFunction.of(ConsumerRecord::key, keyType)));
    }

    /**
     * Creates a ... Equivalent to {@code ObjectProcessor.map(ConsumerRecord::key, keyProcessor)}.
     *
     * @param keyProcessor the key processor
     * @return the consumer record processor
     * @param <K> the key type
     * @see ConsumerRecord#key()
     * @see ObjectProcessor#map(Function, ObjectProcessor)
     */
    public static <K, V> ObjectProcessor<ConsumerRecord<K, V>> key(ObjectProcessor<K> keyProcessor) {
        return ObjectProcessor.map(ConsumerRecord::key, keyProcessor);
    }

    public static <K, V> ObjectProcessor<ConsumerRecord<K, V>> value(GenericType<V> valueType) {
        return ObjectProcessorFunctions.of(List.of(ToObjectFunction.of(ConsumerRecord::value, valueType)));
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
    public static <K, V> ObjectProcessor<ConsumerRecord<K, V>> value(ObjectProcessor<V> valueProcessor) {
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
