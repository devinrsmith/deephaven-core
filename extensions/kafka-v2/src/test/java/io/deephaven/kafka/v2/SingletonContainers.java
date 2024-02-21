/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * <a href=
 * "https://java.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers">Singleton
 * containers</a>
 * <a href="https://testcontainers.com/guides/testcontainers-container-lifecycle/#_using_singleton_containers">Using
 * Singleton Containers</a>
 */
final class SingletonContainers {

    static final class Kafka {

        static KafkaContainer _7_3() {
            return Kafka_7_3.KAFKA;
        }

        static KafkaContainer _7_4() {
            return Kafka_7_4.KAFKA;
        }

        static KafkaContainer _7_5() {
            return Kafka_7_5.KAFKA;
        }

        static KafkaContainer _7_6() {
            return Kafka_7_6.KAFKA;
        }

        private static final class Kafka_7_3 {
            private static final KafkaContainer KAFKA =
                    new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.5")).withKraft();
            static {
                KAFKA.start();
            }
        }

        private static final class Kafka_7_4 {
            private static final KafkaContainer KAFKA =
                    new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.2")).withKraft();
            static {
                KAFKA.start();
            }
        }

        private static final class Kafka_7_5 {
            private static final KafkaContainer KAFKA =
                    new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1")).withKraft();
            static {
                KAFKA.start();
            }
        }

        private static final class Kafka_7_6 {
            private static final KafkaContainer KAFKA =
                    new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0")).withKraft();
            static {
                KAFKA.start();
            }
        }
    }

    static final class Redpanda {
        static RedpandaContainer v23_1() {
            return Redpanda_v23_1.REDPANDA;
        }

        static RedpandaContainer v23_2() {
            return Redpanda_v23_2.REDPANDA;
        }

        static RedpandaContainer v23_3() {
            return Redpanda_v23_3.REDPANDA;
        }

        private static final class Redpanda_v23_1 {
            private static final RedpandaContainer REDPANDA =
                    new RedpandaContainer(DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.1.21"));
            static {
                REDPANDA.start();
            }
        }

        private static final class Redpanda_v23_2 {
            private static final RedpandaContainer REDPANDA =
                    new RedpandaContainer(DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.2.23"));
            static {
                REDPANDA.start();
            }
        }

        private static final class Redpanda_v23_3 {
            private static final RedpandaContainer REDPANDA =
                    new RedpandaContainer(DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.3.3"));
            static {
                REDPANDA.start();
            }
        }
    }

}
