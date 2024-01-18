/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.BuildableStyle;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * The options for creating a {@link KafkaConsumer}.
 *
 * @param <K> the key type
 * @param <V>
 */
@Immutable
@BuildableStyle
public abstract class ClientOptions<K, V> {

    public static <K, V> Builder<K, V> builder() {
        return ImmutableClientOptions.builder();
    }

    public static Map<String, String> load(Path path) throws IOException {
        final Properties properties = new Properties();
        try (final InputStream in = Files.newInputStream(path)) {
            // load buffers internally
            properties.load(in);
        }
        final Map<String, String> map = new HashMap<>(properties.size());
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            map.put((String) entry.getKey(), (String) entry.getValue());
        }
        return map;
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

    final KafkaConsumer<K, V> createClient(Map<String, String> extraOptions) {
        final Map<String, String> config;
        if (extraOptions == null || extraOptions.isEmpty()) {
            config = config();
        } else {
            config = new HashMap<>(config());
            config.putAll(extraOptions);
        }
        // noinspection unchecked,rawtypes
        return new KafkaConsumer<>(
                (Map<String, Object>) (Map) config,
                keyDeserializer().orElse(null),
                valueDeserializer().orElse(null));
    }
}
