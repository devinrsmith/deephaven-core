/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Rule;
import org.testcontainers.redpanda.RedpandaContainer;

import java.util.Map;

// todo: make out of band?
public final class TableOptionsRedpanda233Test extends TableOptionsTestBase {

    @Rule
    public final RedpandaContainer redpanda =
            new RedpandaContainer("docker.redpanda.com/redpandadata/redpanda:v23.3.3");

    @Override
    public Map<String, Object> adminConfig() {
        return Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, redpanda.getBootstrapServers());
    }

    @Override
    public Map<String, Object> producerConfig() {
        return Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, redpanda.getBootstrapServers());
    }

    @Override
    public Map<String, String> clientConfig() {
        return Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, redpanda.getBootstrapServers());
    }
}
