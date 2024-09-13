//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.deephaven.api.util.NameValidator;
import io.deephaven.configuration.ConfigDir;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.kafka.github.StarHandler;
import io.deephaven.kafka.github.WebhookHandler;
import io.deephaven.processor.factory.EventProcessorFactories;
import io.deephaven.processor.factory.EventProcessorFactory;
import io.deephaven.processor.factory.EventProcessorFactory.EventProcessor;
import io.deephaven.processor.factory.EventProcessorStreamSpec;
import io.deephaven.processor.factory.EventProcessors;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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

    private static final Logger log = LoggerFactory.getLogger(MyKafkaSpecifics.class);

    private static final StreamKey BASIC = new StreamKey();
    private static final StreamKey HEADERS = new StreamKey();

    public static Table consume1(EventProcessorFactory<ConsumerRecord<?, ?>> factory) throws IOException {
        if (factory.specs().size() != 1) {
            throw new IllegalArgumentException();
        }
        final EventProcessorStreamSpec spec = factory.specs().iterator().next();
        final Keys keys = spec.keys();
        final SingleBlinkCoordinator coordinator = new SingleBlinkCoordinator(keys);
        final TableDefinition td = TableDefinition.of(keys.keys().stream()
                .map(k -> ColumnDefinition.of(NameValidator.legalizeColumnName(k.toString()), k.type()))
                .collect(Collectors.toList()));
        final StreamToBlinkTableAdapter adapter =
                new StreamToBlinkTableAdapter(td, coordinator, ExecutionContext.getContext().getUpdateGraph(), "test");
        final Sink sink = Sink.builder()
                .coordinator(coordinator)
                .putStreams(spec.key(), coordinator)
                .build();
        final Properties properties = props();
        final Thread thread = new Thread(() -> {
            try (
                    final KafkaConsumer<?, ?> consumer =
                            new KafkaConsumer<>(properties, new VoidDeserializer(), new StringDeserializer());
                    final EventProcessor<ConsumerRecord<?, ?>> processor = factory.create(sink)) {
                consumer.assign(List.of(new TopicPartition("gh-org-webhook-deephaven", 0)));
                consumer.seekToEnd(List.of(new TopicPartition("gh-org-webhook-deephaven", 0)));
                while (true) {
                    final ConsumerRecords<?, ?> records = consumer.poll(Duration.ofMillis(100));
                    if (records.isEmpty()) {
                        continue;
                    }
                    coordinator.writing();
                    for (ConsumerRecord<?, ?> record : records) {
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

    private static Properties props() throws IOException {
        final Properties props = new Properties();
        try (final InputStream is = Files.newInputStream(path("kafka.properties"))) {
            // load buffers internally
            props.load(is);
        }
        return props;
    }

    private static Path path(String name) {
        return ConfigDir.get().map(p -> p.resolve("creds")).map(p -> p.resolve(name)).orElseThrow();
    }

    public static EventProcessorFactory<ConsumerRecord<?, ?>> stars() {
        return EventProcessorFactories.of(EventProcessorStreamSpec.builder()
                .key(StarHandler.KEY)
                .usesCoordinator(false)
                .expectedSize(0)
                .isRowOriented(true)
                .keys(Keys.builder().addKeys(
                        KafkaFactory.TIMESTAMP,
                        StarHandler.ACTION,
                        StarHandler.STARRED_AT,
                        StarHandler.ORGANIZATION,
                        StarHandler.REPOSITORY,
                        StarHandler.SENDER,
                        StarHandler.SENDER_AVATAR).build())
                .build(),
                stream -> {
                    final ObjectMapper objectMapper = JsonMapper.builder()
                            .addModule(new JavaTimeModule())
                            .build();
                    final WebhookHandler webhookHandler = WebhookHandler.from(stream, objectMapper);
                    final RecordConsumer basicsHandler = KafkaFactory.basics(stream);
                    return EventProcessors.noClose(record -> {
                        try {
                            if (!webhookHandler.handle(record)) {
                                // skip
                                return;
                            }
                        } catch (IOException e) {
                            log.error(e).append("Error processing record").endl();
                            return;
                        }
                        basicsHandler.accept(record);
                        stream.advanceAll();
                    });
                });
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
                        KafkaFactory.VALUE,
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
                        KafkaFactory.HEADER_VALUE_STR).build())
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
