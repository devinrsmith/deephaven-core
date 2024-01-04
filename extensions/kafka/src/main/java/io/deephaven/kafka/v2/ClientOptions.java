/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.BuildableStyle;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.immutables.value.Value.Immutable;

import java.util.Properties;

@Immutable
@BuildableStyle
public abstract class ClientOptions<K, V> {

    public abstract Properties config();

    public abstract Deserializer<K> keyDeserializer();

    public abstract Deserializer<V> valueDeserializer();

    final KafkaConsumer<K, V> createClient() {
        return new KafkaConsumer<>(config(), keyDeserializer(), valueDeserializer());
    }
}
