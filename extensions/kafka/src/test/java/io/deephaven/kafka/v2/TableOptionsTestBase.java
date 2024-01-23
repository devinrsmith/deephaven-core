/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.kafka.KafkaTools.ConsumerLoopCallback;
import io.deephaven.kafka.v2.TableOptions.OpinionatedClientOptions;
import io.deephaven.kafka.v2.TableOptions.OpinionatedRecordOptions;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public abstract class TableOptionsTestBase {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    public abstract Map<String, Object> adminConfig();

    public abstract Map<String, Object> producerConfig();

    public abstract Map<String, String> clientConfig();

    @Test
    public void keyValue() throws ExecutionException, InterruptedException {
        try (final Admin admin = Admin.create(adminConfig())) {
            admin.createTopics(List.of(new NewTopic("MyTest", Optional.of(1), Optional.empty()))).all().get();
        }
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(),
                new StringSerializer(),
                new StringSerializer())) {
            producer.send(new ProducerRecord<>("MyTest", "mykey", "myvalue")).get();
        }
        final FirstPollCompleted firstPollCompleted = new FirstPollCompleted();
        try (final StreamToBlinkTableAdapter adapter = TableOptions.<String, String>builder()
                .clientOptions(ClientOptions.<String, String>builder()
                        .putAllConfig(clientConfig())
                        .keyDeserializer(new StringDeserializer())
                        .valueDeserializer(new StringDeserializer())
                        .build())
                .opinionatedClientOptions(OpinionatedClientOptions.none())
                .addOffsets(Offsets.beginning("MyTest"))
                .receiveTimestamp(null)
                .recordOptions(ConsumerRecordOptions.empty())
                .opinionatedRecordOptions(OpinionatedRecordOptions.none())
                .keyProcessor(ObjectProcessor.simple(Type.stringType()))
                .valueProcessor(ObjectProcessor.simple(Type.stringType()))
                .callback(firstPollCompleted)
                .build()
                .adapter()) {
            firstPollCompleted.await();
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(adapter::run);
            final Table expected = TableTools.newTable(
                    TableTools.stringCol("Key", "mykey"),
                    TableTools.stringCol("Value", "myvalue"));
            TstUtils.assertTableEquals(expected, adapter.table());
        }
    }

    @Test
    public void keyValue2() throws ExecutionException, InterruptedException {
        keyValue();
    }

    static class FirstPollCompleted implements ConsumerLoopCallback {
        private final CountDownLatch afterPollLatch = new CountDownLatch(1);

        public void await() throws InterruptedException {
            afterPollLatch.await();
        }

        @Override
        public void beforePoll(KafkaConsumer<?, ?> consumer) {

        }

        @Override
        public void afterPoll(KafkaConsumer<?, ?> consumer, boolean more) {
            afterPollLatch.countDown();
        }
    }
}
