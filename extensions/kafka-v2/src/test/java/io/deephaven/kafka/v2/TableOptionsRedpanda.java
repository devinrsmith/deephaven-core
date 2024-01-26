/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.kafka.v2.SingletonContainers.Redpanda;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.testcontainers.redpanda.RedpandaContainer;

import java.util.Map;

// todo: make out of band?
@SuppressWarnings("resource")
public abstract class TableOptionsRedpanda extends TableOptionsBase {

    public abstract RedpandaContainer container();

    @Override
    public Map<String, Object> adminConfig() {
        return Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, container().getBootstrapServers());
    }

    @Override
    public Map<String, Object> producerConfig() {
        return Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, container().getBootstrapServers());
    }

    @Override
    public Map<String, String> clientConfig() {
        return Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, container().getBootstrapServers());
    }

    @Override
    public int initialLeaderEpoch() {
        return 1;
    }

    public static class v23_1 extends TableOptionsRedpanda {

        @Override
        public RedpandaContainer container() {
            return Redpanda.v23_1();
        }
    }

    public static class v23_2 extends TableOptionsRedpanda {

        @Override
        public RedpandaContainer container() {
            return Redpanda.v23_2();
        }
    }

    public static class v23_3 extends TableOptionsRedpanda {

        @Override
        public RedpandaContainer container() {
            return Redpanda.v23_3();
        }
    }
}
