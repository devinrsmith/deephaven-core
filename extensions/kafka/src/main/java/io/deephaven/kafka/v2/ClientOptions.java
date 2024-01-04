/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.BuildableStyle;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.immutables.value.Value.Immutable;

import java.util.Map;
import java.util.Optional;

@Immutable
@BuildableStyle
public abstract class ClientOptions<K, V> {

    public static <K, V> Builder<K, V> builder() {
        return ImmutableClientOptions.builder();
    }

    public abstract Map<String, String> config();

    public abstract Optional<Deserializer<K>> keyDeserializer();

    public abstract Optional<Deserializer<V>> valueDeserializer();

    public interface Builder<K, V> {

        Builder<K, V> putConfig(String key, String value);

        Builder<K, V> putConfig(Map.Entry<String, ? extends String> entry);

        Builder<K, V> putAllConfig(Map<String, ? extends String> entries);

        Builder<K, V> keyDeserializer(Deserializer<K> keyDeserializer);

        Builder<K, V> valueDeserializer(Deserializer<V> valueDeserializer);

        ClientOptions<K, V> build();
    }

    final KafkaConsumer<K, V> createClient() {
        // noinspection unchecked,rawtypes
        return new KafkaConsumer<>(
                (Map<String, Object>) (Map) config(),
                keyDeserializer().orElse(null),
                valueDeserializer().orElse(null));
    }
}
