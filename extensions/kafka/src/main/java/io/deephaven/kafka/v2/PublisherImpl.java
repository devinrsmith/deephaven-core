/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.base.clock.Clock;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.util.SafeCloseable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

final class PublisherImpl<K, V> implements Publisher {
    private final Set<TopicPartition> topicPartitions;
    private final Predicate<ConsumerRecord<K, V>> filter;
    private final ObjectProcessor<ConsumerRecord<K, V>> processor;
    private final Runnable onShutdown;
    private final WritableObjectChunk<ConsumerRecord<K, V>, ?> chunk;
    private final int chunkSize;
    private int chunkPos = 0;
    private WritableLongChunk<?> receiveTimestampChunk;
    private StreamConsumer streamConsumer;

    PublisherImpl(
            Set<TopicPartition> topicPartitions,
            Predicate<ConsumerRecord<K, V>> filter,
            ObjectProcessor<ConsumerRecord<K, V>> processor,
            Runnable onShutdown,
            int chunkSize,
            boolean receiveTimestamp) {
        this.topicPartitions = Set.copyOf(topicPartitions);
        this.filter = Objects.requireNonNull(filter);
        this.processor = Objects.requireNonNull(processor);
        this.onShutdown = Objects.requireNonNull(onShutdown);
        this.chunkSize = chunkSize;
        this.chunk = WritableObjectChunk.makeWritableChunk(chunkSize);
        this.receiveTimestampChunk = receiveTimestamp ? WritableLongChunk.makeWritableChunk(chunkSize) : null;
    }

    @Override
    public Set<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        this.streamConsumer = Objects.requireNonNull(consumer);
    }

    @Override
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

    @Override
    public void shutdown() {
        onShutdown.run();
    }

    boolean hasStreamConsumer() {
        return streamConsumer != null;
    }

    void accept(ConsumerRecords<K, V> records) {
        final long receiveTimeEpochNanos = receiveTimestampChunk != null
                ? Clock.system().currentTimeNanos()
                : 0;
        synchronized (this) {
            for (TopicPartition topicPartition : records.partitions()) {
                fillImpl(receiveTimeEpochNanos, topicPartition, records.records(topicPartition));
            }
        }
    }

    synchronized void acceptFailure(Throwable cause) {
        streamConsumer.acceptFailure(cause);
    }

    void close() {
        if (receiveTimestampChunk != null) {
            receiveTimestampChunk.close();
        }
        chunk.close();
    }

    synchronized void fillImpl(long receiveTimeEpochNanos, TopicPartition topicPartition,
            List<ConsumerRecord<K, V>> records) {
        // todo: check topicPartition?
        final Iterator<ConsumerRecord<K, V>> it = records.iterator();
        boolean didFlush = false;
        try {
            while (true) {
                while (chunkPos < chunkSize && it.hasNext()) {
                    final ConsumerRecord<K, V> record = it.next();
                    if (!filter.test(record)) {
                        continue;
                    }
                    if (receiveTimestampChunk != null) {
                        receiveTimestampChunk.set(chunkPos, receiveTimeEpochNanos);
                    }
                    chunk.set(chunkPos, record);
                    ++chunkPos;
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
        final WritableChunk<Values>[] allChunks;
        final List<WritableChunk<?>> processorChunks;
        if (receiveTimestampChunk != null) {
            receiveTimestampChunk.setSize(chunkPos);
            // Already called by makeWritableChunk
            // receiveTimestampChunk.setSize(chunkPos);
            // noinspection unchecked
            allChunks =
                    Stream.concat(Stream.of(receiveTimestampChunk), newProcessorChunks()).toArray(WritableChunk[]::new);
            processorChunks = Arrays.<WritableChunk<?>>asList(allChunks).subList(1, allChunks.length);
        } else {
            // noinspection unchecked
            allChunks = newProcessorChunks().toArray(WritableChunk[]::new);
            processorChunks = Arrays.asList(allChunks);
        }
        chunk.setSize(chunkPos);
        try {
            processor.processAll(chunk, processorChunks);
        } catch (Throwable t) {
            closeAll(t, allChunks);
            throw t;
        }
        try {
            streamConsumer.accept(allChunks);
        } finally {
            chunkPos = 0;
            receiveTimestampChunk = WritableLongChunk.makeWritableChunk(chunkSize);
        }
    }

    private Stream<WritableChunk<Any>> newProcessorChunks() {
        return processor
                .outputTypes()
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(chunkType -> chunkType.makeWritableChunk(chunkPos))
                .peek(wc -> wc.setSize(0));
    }

    private static void closeAll(Throwable t, SafeCloseable... out) {
        try {
            SafeCloseable.closeAll(out);
        } catch (Throwable t2) {
            t.addSuppressed(t2);
        }
    }
}
