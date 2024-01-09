/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.kafka.KafkaTools;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.util.thread.NamingThreadFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.deephaven.kafka.v2.KafkaTableOptions.predicateTrue;

@Immutable
@BuildableStyle
public abstract class KafkaOptions<K, V> {

    public static Publishers create(KafkaOptions<?, ?> options) {
        return options.publishers();
    }

    public static <T> T create(KafkaOptions<?, ?> options, Function<Map<StreamPublisher, Set<TopicPartition>>, T> function) {
        try (final Publishers publishers = create(options)) {
            try {
                final T t = function.apply(publishers.publishers());
                publishers.start();
                return t;
            } catch (Throwable throwable) {
                publishers.errorBeforeStart(throwable);
                throw throwable;
            }
        }
    }


    /**
     * The Kafka client options.
     *
     * @return the Kafka client options
     */
    public abstract ClientOptions<K, V> clientOptions();

    /**
     * If an opinionated client configuration should be used to extend {@link #clientOptions()}. By default, is
     * {@code true}.
     *
     * <p>
     * The specifics of these may change from release to release. Callers wishing to do their own optimizations are
     * encouraged to set this to {@code false} and finely configure {@link #clientOptions()}. Currently, consists of:
     *
     * <ul>
     * <li>If unset, sets {@link ConsumerConfig#CLIENT_ID_CONFIG} to {@link #name()}.</li>
     * <li>If unset, sets {@link ConsumerConfig#DEFAULT_API_TIMEOUT_MS_CONFIG} to 5 seconds.</li>
     * <li>If unset, sets {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG} to {@link #chunkSize()}.</li>
     * <li>If unset, sets {@link ConsumerConfig#MAX_PARTITION_FETCH_BYTES_CONFIG} to 16 MiB.</li>
     * <li>If unset, sets {@link ClientOptions#keyDeserializer()} to {@link ByteArrayDeserializer}
     * ({@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} must also be unset).</li>
     * <li>If unset, sets {@link ClientOptions#valueDeserializer()} to {@link ByteArrayDeserializer}
     * ({@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG} must also be unset).</li>
     * </ul>
     *
     * @return if opinionated client configuration should be used
     */
    @Default
    public boolean useOpinionatedClientOptions() {
        return true;
    }


    /**
     * The offsets.
     *
     * @return the offsets
     */
    public abstract Offsets offsets();

    /**
     * The record filter. By default, is equivalent to {@code record -> true}, which will include all records.
     *
     * @return the record filter
     */
    @Default
    public Predicate<ConsumerRecord<K, V>> filter() {
        return predicateTrue();
    }

    // todo: give easy way for users to construct w/ specs for specific key / value types

    /**
     * The record processor.
     *
     * @return
     */
    public abstract ObjectProcessor<ConsumerRecord<K, V>> processor();

    public abstract Partitioning partitioning();

    public interface Partitioning {

        static Partitioning single() {
            return null;
        }

        static Partitioning perPartition() {
            return null;
        }

        static Partitioning perTopic() {
            return null;
        }

        static Partitioning perTopicPartition() {
            return null;
        }
    }

    public interface Publishers extends Closeable {
        Map<StreamPublisher, Set<TopicPartition>> publishers();

        void start();

        void errorBeforeStart(Throwable t);

        @Override
        void close();
    }


    final Publishers publishers() {

        final KafkaPublisherDriver<K, V> publisher = publisher();
        return new Publishers() {
            @Override
            public Map<StreamPublisher, Set<TopicPartition>> publishers() {
                return Map.of(publisher, null);
            }

            @Override
            public void start() {

            }

            @Override
            public void errorBeforeStart(Throwable t) {

            }

            @Override
            public void close() {

            }
        };
    }

    private KafkaPublisherDriver<K, V> publisher() {
        return KafkaPublisherDriver.of(
                useOpinionatedClientOptions() ? opinionatedClientOptions() : clientOptions(),
                offsets(),
                new KafkaStreamConsumerAdapter<>(filter(), processor(), 2048, true),
                threadFactory(),
                callback());
    }

    private ClientOptions<K, V> opinionatedClientOptions() {
        final HashMap<String, String> config = new HashMap<>(clientOptions().config());
        final ClientOptions.Builder<K, V> opinionated = ClientOptions.builder();
        if (!config.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)) {
            config.put(ConsumerConfig.CLIENT_ID_CONFIG, name());
        }
        if (!config.containsKey(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)) {
            // This is local only option, doesn't affect server.
            // Default is 500
            // This affects the maximum number of records that io.deephaven.kafka.v2.KafkaPublisherDriver.runOnce will
            // receive at once. There's a small tradeoff here; allowing enough for runOnce to do a flush, but also small
            // enough to minimize potential sync wait for StreamPublisher#flush calls (cycle).
            config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(chunkSize()));
        }
        if (!config.containsKey(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)) {
            // The default of 60 seconds seems high
            config.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Long.toString(Duration.ofSeconds(5).toMillis()));
        }
        if (!config.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
            // Todo: should we be stricter than kafka default?
            // config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.NONE.toString());
        }
        if (!config.containsKey(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG)) {
            // Potential to base as percentage of cycle time? Might be more reasonable to encourage user to set?
        }
        if (!config.containsKey(ConsumerConfig.FETCH_MIN_BYTES_CONFIG)) {
            // Potential to configure for higher throughput; still adheres to max wait ms
        }
        if (!config.containsKey(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG)) {
            // The default of 1 MiB seems too low
            // Overall limit still adheres to FETCH_MAX_BYTES_CONFIG (default 50 MiB)
            config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Integer.toString(16 * 1024 * 1024));
        }
        if (clientOptions().keyDeserializer().isPresent()) {
            opinionated.keyDeserializer(clientOptions().keyDeserializer().get());
        } else if (!config.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            // This is not safe; we are assuming the user would have set the deserializer or config directly if they
            // actually cared about the type. We'll get a cast exception at runtime if the user is actually trying to
            // process it as a different type.
            // noinspection unchecked
            opinionated.keyDeserializer((Deserializer<K>) new ByteArrayDeserializer());
        }
        if (clientOptions().valueDeserializer().isPresent()) {
            opinionated.valueDeserializer(clientOptions().valueDeserializer().get());
        } else if (!config.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            // This is not safe; we are assuming the user would have set the deserializer or config directly if they
            // actually cared about the type. We'll get a cast exception at runtime if the user is actually trying to
            // process it as a different type.
            // noinspection unchecked
            opinionated.valueDeserializer((Deserializer<V>) new ByteArrayDeserializer());
        }
        return opinionated.putAllConfig(config).build();
    }


    // Maybe expose as configuration option in future?
    private static ThreadFactory threadFactory() {
        return new NamingThreadFactory(null, KafkaTableOptions.class, "KafkaPublisherDriver", true);
    }

    // Maybe expose as configuration option in future?
    private static KafkaTools.ConsumerLoopCallback callback() {
        return null;
    }
}
