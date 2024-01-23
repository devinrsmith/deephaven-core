/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public abstract class TableOptionsKafkaTestBase extends TableOptionsTestBase {

    public abstract String version();

    @Rule
    public KafkaContainer kafka;

    @Before
    public void setUp() {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + version()));
        kafka.start();
    }

    @After
    public void tearDown() {
        kafka.stop();
    }

    @Override
    public Map<String, Object> adminConfig() {
        return Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    }

    @Override
    public Map<String, Object> producerConfig() {
        return Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    }

    @Override
    public Map<String, String> clientConfig() {
        return Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    }
}
