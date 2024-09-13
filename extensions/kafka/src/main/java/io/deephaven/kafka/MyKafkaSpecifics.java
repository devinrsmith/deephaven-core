//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.processor.factory.EventProcessorFactories;
import io.deephaven.processor.factory.EventProcessorFactory;
import io.deephaven.processor.factory.EventProcessorFactory.EventProcessor;
import io.deephaven.processor.factory.EventProcessorStreamSpec;
import io.deephaven.processor.sink.Key;
import io.deephaven.processor.sink.Keys;
import io.deephaven.processor.sink.Sink;
import io.deephaven.processor.sink.Sink.StreamKey;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.ObjectAppender;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.SingleBlinkCoordinator;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public class MyKafkaSpecifics {

    private static final String GITHUB_EVENT = "X-GitHub-Event";
    private static final String GITHUB_DELIVERY = "X-GitHub-Delivery";
    private static final Key<String> GITHUB_EVENT_KEY = Key.of(GITHUB_EVENT, Type.stringType());
    private static final Key<String> GITHUB_DELIVERY_KEY = Key.of(GITHUB_DELIVERY, Type.stringType());

    private static final StreamKey BASIC = new StreamKey();
    private static final StreamKey HEADERS = new StreamKey();

    public static Table consume1(EventProcessorFactory<ConsumerRecord<?, ?>> factory) {
        if (factory.specs().size() != 1) {
            throw new IllegalArgumentException();
        }
        final EventProcessorStreamSpec spec = factory.specs().iterator().next();
        final Keys keys = spec.keys();
        final SingleBlinkCoordinator coordinator = new SingleBlinkCoordinator(keys);
        final TableDefinition td = TableDefinition.of(keys.keys().stream()
                .map(k -> ColumnDefinition.of(k.toString(), k.type())).collect(Collectors.toList()));
        final StreamToBlinkTableAdapter adapter =
                new StreamToBlinkTableAdapter(td, coordinator, ExecutionContext.getContext().getUpdateGraph(), "test");
        final Sink sink = Sink.builder()
                .coordinator(coordinator)
                .putStreams(spec.key(), coordinator)
                .build();
        final Thread thread = new Thread(() -> {
            try (
                    final KafkaConsumer<Void, byte[]> consumer =
                            new KafkaConsumer<>(props(), new VoidDeserializer(), new ByteArrayDeserializer());
                    final EventProcessor<ConsumerRecord<?, ?>> processor = factory.create(sink)) {
                consumer.assign(List.of(new TopicPartition("gh-org-webhook-deephaven", 0)));
                consumer.seekToBeginning(List.of(new TopicPartition("gh-org-webhook-deephaven", 0)));
                while (true) {
                    final ConsumerRecords<Void, byte[]> records = consumer.poll(Duration.ofMillis(100));
                    if (records.isEmpty()) {
                        continue;
                    }
                    coordinator.writing();
                    for (ConsumerRecord<Void, byte[]> record : records) {
                        coordinator.yield();
                        processor.writeToSink(record);
                    }
                    coordinator.sync();
                }
            }
        });
        thread.start();
        return adapter.table();
    }

    private static Properties props() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "https://driving-wombat-12446-us1-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ZHJpdmluZy13b21iYXQtMTI0NDYk0lCNNur3U0ntg-mv3yhYxBV-2x07JMyrjqw\" password=\"NGY1ZWE1ZmUtNTkwOC00OWVlLWEzNDMtYTM5YjQ1NjYxMzNk\";");
        return props;
    }

    public static EventProcessorFactory<ConsumerRecord<?, ?>> basics() {
        return EventProcessorFactories.of(EventProcessorStreamSpec.builder()
                .key(BASIC)
                .usesCoordinator(false)
                .expectedSize(1)
                .isRowOriented(true)
                .keys(Keys.builder().addKeys(
                        KafkaFactory.TOPIC,
                        KafkaFactory.PARTITION,
                        KafkaFactory.OFFSET,
                        KafkaFactory.TIMESTAMP,
                        GITHUB_EVENT_KEY,
                        GITHUB_DELIVERY_KEY).build())
                .build(),
                stream -> KafkaFactory.records(stream, KafkaFactory
                        .key(stream)
                        .andThen(KafkaFactory.basics(stream))
                        .andThen(special(stream))));
    }

    public static EventProcessorFactory<ConsumerRecord<?, ?>> headers() {
        return EventProcessorFactories.of(EventProcessorStreamSpec.builder()
                .key(HEADERS)
                .usesCoordinator(false)
                .expectedSize(8)
                .isRowOriented(true)
                .keys(Keys.builder().addKeys(
                        KafkaFactory.TOPIC,
                        KafkaFactory.PARTITION,
                        KafkaFactory.OFFSET,
                        KafkaFactory.HEADER_INDEX,
                        KafkaFactory.HEADER_KEY,
                        KafkaFactory.HEADER_VALUE).build())
                .build(),
                stream -> KafkaFactory.headers(stream,
                        HeaderConsumer.wrap(KafkaFactory.key(stream)).andThen(KafkaFactory.header(stream))));
    }

    public static RecordConsumer special(Stream stream) {
        final ObjectAppender<String> event = ObjectAppender.get(stream, GITHUB_EVENT_KEY);
        final ObjectAppender<String> delivery = ObjectAppender.get(stream, GITHUB_DELIVERY_KEY);
        return new Special(event, delivery);
    }

    private static class Special implements RecordConsumer {

        private final ObjectAppender<String> event;
        private final ObjectAppender<String> delivery;

        private Special(ObjectAppender<String> event, ObjectAppender<String> delivery) {
            this.event = Objects.requireNonNull(event);
            this.delivery = Objects.requireNonNull(delivery);
        }

        @Override
        public void accept(ConsumerRecord<?, ?> record) {
            boolean s1 = false;
            boolean s2 = false;
            for (Header header : record.headers()) {
                switch (header.key()) {
                    case GITHUB_EVENT:
                        s1 = true;
                        event.set(new String(header.value(), StandardCharsets.UTF_8));
                        break;
                    case GITHUB_DELIVERY:
                        s2 = true;
                        delivery.set(new String(header.value(), StandardCharsets.UTF_8));
                        break;
                }
            }
            if (!s1) {
                event.setNull();
            }
            if (!s2) {
                delivery.setNull();
            }
        }
    }
}
