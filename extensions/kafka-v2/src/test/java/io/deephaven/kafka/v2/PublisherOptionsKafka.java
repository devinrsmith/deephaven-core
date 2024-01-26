/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.kafka.v2.SingletonContainers.Kafka;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.testcontainers.containers.KafkaContainer;

import java.util.Map;

// todo: make out of band?
@SuppressWarnings("resource")
public abstract class PublisherOptionsKafka extends PublishersOptionsBase {

    public abstract KafkaContainer container();

    @Override
    public Map<String, Object> adminConfig() {
        return Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, container().getBootstrapServers());
    }

    @Override
    public Map<String, Object> producerConfig() {
        return Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, container().getBootstrapServers());
    }

    @Override
    public Map<String, String> consumerConfig() {
        return Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, container().getBootstrapServers());
    }

    // todo: make out of band?
    public static class _7_3 extends PublisherOptionsKafka {

        @Override
        public KafkaContainer container() {
            return Kafka._7_3();
        }
    }

    // todo: make out of band?
    public static class _7_4 extends PublisherOptionsKafka {

        @Override
        public KafkaContainer container() {
            return Kafka._7_4();
        }
    }

    // todo: make out of band?
    public static class _7_5 extends PublisherOptionsKafka {

        @Override
        public KafkaContainer container() {
            return Kafka._7_5();
        }
    }
}
