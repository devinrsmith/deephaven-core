/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.util.SafeCloseable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

class KafkaStreamConsumerAdapter<K, V> {
    private final ObjectProcessor<ConsumerRecord<K, V>> processor;
    private final WritableObjectChunk<ConsumerRecord<K, V>, ?> chunk;
    private final int chunkSize;
    private int chunkPos = 0;
    private StreamConsumer streamConsumer;


    KafkaStreamConsumerAdapter(
            ObjectProcessor<ConsumerRecord<K, V>> processor,
            int chunkSize) {
        this.processor = Objects.requireNonNull(processor);
        this.chunkSize = chunkSize;
        this.chunk = WritableObjectChunk.makeWritableChunk(chunkSize); // todo: close?
    }

    void init(StreamConsumer streamConsumer) {
        this.streamConsumer = Objects.requireNonNull(streamConsumer);
    }

    boolean hasStreamConsumer() {
        return streamConsumer != null;
    }

    public synchronized void accept(ConsumerRecords<K, V> records) {
        for (TopicPartition partition : records.partitions()) {
            fillImpl(partition, records.records(partition));
        }
    }

    public synchronized void flush() {
        final int initialChunkPos;
        if ((initialChunkPos = chunkPos) == 0) {
            return;
        }
        try {
            doFlush();
        } finally {
            chunk.fillWithNullValue(chunkPos, initialChunkPos);
        }
    }

    public synchronized void acceptFailure(Throwable cause) {
        streamConsumer.acceptFailure(cause);
    }

    private void fillImpl(TopicPartition topicPartition, List<ConsumerRecord<K, V>> records) {
        final Iterator<ConsumerRecord<K, V>> it = records.iterator();
        boolean didFlush = false;
        try {
            while (true) {
                for (; chunkPos < chunkSize && it.hasNext(); ++chunkPos) {
                    // todo: assert topic / partition?
                    chunk.set(chunkPos, it.next());
                }
                if (chunkPos < chunkSize) {
                    break;
                }
                doFlush();
                didFlush = true;
            }
        } finally {
            if (didFlush) {
                chunk.fillWithNullValue(chunkPos, chunkSize - chunkPos);
            }
        }
    }

    private void doFlush() {
        chunk.setSize(chunkPos);
        // noinspection unchecked
        final WritableChunk<Values>[] out = processor
                .outputTypes()
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(chunkType -> chunkType.makeWritableChunk(chunkPos))
                .toArray(WritableChunk[]::new);
        for (WritableChunk<Values> wc : out) {
            wc.setSize(0);
        }
        try {
            processor.processAll(chunk, Arrays.asList(out));
        } catch (Throwable t) {
            closeAll(t, out);
            throw t;
        }
        chunkPos = 0;
        streamConsumer.accept(Collections.singleton(out));
    }

    private static void closeAll(Throwable t, SafeCloseable[] out) {
        try {
            SafeCloseable.closeAll(out);
        } catch (Throwable t2) {
            t.addSuppressed(t2);
        }
    }
}
