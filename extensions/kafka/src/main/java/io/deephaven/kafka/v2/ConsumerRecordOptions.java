/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Provides options for processing the common fields of {@link ConsumerRecord}.
 */
@Immutable
@BuildableStyle
public abstract class ConsumerRecordOptions {

    public static Builder builder() {
        return ImmutableConsumerRecordOptions.builder();
    }

    /**
     * The latest record options.
     *
     * @return
     */
    public static ConsumerRecordOptions latest() {
        return builder().build();
    }

    public static ConsumerRecordOptions empty() {
        return builder()
                .topic(null)
                .partition(null)
                .offset(null)
                .leaderEpoch(null)
                .timestampType(null)
                .timestamp(null)
                .serializedKeySize(null)
                .serializedValueSize(null)
                .build();
    }

    /**
     *
     * @return
     */
    public static ConsumerRecordOptions v1() {
        return builder()
                .topic(null)
                .partition("KafkaPartition")
                .offset("KafkaOffset")
                .leaderEpoch(null)
                .timestampType(null)
                .timestamp("KafkaTimestamp")
                .serializedKeySize(null)
                .serializedValueSize(null)
                .build();
    }

    public static ConsumerRecordOptions v2() {
        return builder()
                .topic("Topic")
                .build();
    }

    @Default
    @Nullable
    public abstract Optional<String> topic();

    @Default
    @Nullable
    public abstract Optional<String> partition() {
        return "Partition";
    }

    @Default
    @Nullable
    public String offset() {
        return "Offset";
    }

    @Default
    @Nullable
    public String leaderEpoch() {
        return "LeaderEpoch";
    }

    @Default
    @Nullable
    public String timestampType() {
        return "TimestampType";
    }

    @Default
    @Nullable
    public String timestamp() {
        return "Timestamp";
    }

    @Default
    @Nullable
    public String serializedKeySize() {
        return "SerializedKeySize";
    }

    @Default
    @Nullable
    public String serializedValueSize() {
        return "SerializedValueSize";
    }

    // todo: headers? num headers?

    public interface Builder {
        Builder topic(String topic);

        Builder partition(String partition);

        Builder offset(String offset);

        Builder leaderEpoch(String leaderEpoch);

        Builder timestampType(String timestampType);

        Builder timestamp(String timestamp);

        Builder serializedKeySize(String serializedKeySize);

        Builder serializedValueSize(String serializedValueSize);

        ConsumerRecordOptions build();
    }

    @Derived
    List<Type<?>> outputTypes() {
        final List<Type<?>> types = new ArrayList<>();
        if (outputTopic()) {
            types.add(Type.stringType());
        }
        if (outputPartition()) {
            types.add(Type.intType());
        }
        if (outputOffset()) {
            types.add(Type.longType());
        }
        if (outputLeaderEpoch()) {
            types.add(Type.intType());
        }
        if (outputTimestampType()) {
            types.add(Type.ofCustom(TimestampType.class));
        }
        if (outputTimestamp()) {
            types.add(Type.instantType());
        }
        if (outputSerializedKeySize()) {
            types.add(Type.intType());
        }
        if (outputSerializedValueSize()) {
            types.add(Type.intType());
        }
        return Collections.unmodifiableList(types);
    }

    /**
     * Creates a stream the contains the non-{@code null} of {@link #topic()}, {@link #partition()}, {@link #offset()},
     * {@link #leaderEpoch()}, {@link #timestampType()}, {@link #timestamp()}, {@link #serializedKeySize()}, and
     * {@link #serializedValueSize()}. This corresponds with the output types of {@link #processor()}.
     *
     * @return the column names
     */
    public final Stream<String> columnNames() {
        // noinspection RedundantTypeArguments
        return Stream.of(
                Stream.ofNullable(topic()),
                Stream.ofNullable(partition()),
                Stream.ofNullable(offset()),
                Stream.ofNullable(leaderEpoch()),
                Stream.ofNullable(timestampType()),
                Stream.ofNullable(timestamp()),
                Stream.ofNullable(serializedKeySize()),
                Stream.ofNullable(serializedValueSize()))
                .flatMap(Function.<Stream<String>>identity());
    }

    final <K, V> ObjectProcessor<ConsumerRecord<K, V>> processor() {
        return isEmpty() ? ObjectProcessor.empty() : new ConsumerRecordOptionsProcessor<>();
    }

    private boolean isEmpty() {
        return columnNames().allMatch(Objects::isNull);
    }

    private boolean outputTopic() {
        return topic() != null;
    }

    private boolean outputPartition() {
        return partition() != null;
    }

    private boolean outputOffset() {
        return offset() != null;
    }

    private boolean outputLeaderEpoch() {
        return leaderEpoch() != null;
    }

    private boolean outputTimestampType() {
        return timestampType() != null;
    }

    private boolean outputTimestamp() {
        return timestamp() != null;
    }

    private boolean outputSerializedKeySize() {
        return serializedKeySize() != null;
    }

    private boolean outputSerializedValueSize() {
        return serializedValueSize() != null;
    }

    private class ConsumerRecordOptionsProcessor<K, V> implements ObjectProcessor<ConsumerRecord<K, V>> {
        @Override
        public List<Type<?>> outputTypes() {
            return ConsumerRecordOptions.this.outputTypes();
        }

        @Override
        public void processAll(ObjectChunk<? extends ConsumerRecord<K, V>, ?> in, List<WritableChunk<?>> out) {
            int ix = 0;
            final WritableObjectChunk<String, ?> topics = outputTopic() ? out.get(ix++).asWritableObjectChunk() : null;
            final WritableIntChunk<?> partitions = outputPartition() ? out.get(ix++).asWritableIntChunk() : null;
            final WritableLongChunk<?> offsets = outputOffset() ? out.get(ix++).asWritableLongChunk() : null;
            final WritableIntChunk<?> leaderEpochs = outputLeaderEpoch() ? out.get(ix++).asWritableIntChunk() : null;
            final WritableObjectChunk<TimestampType, ?> timestampTypes =
                    outputTimestampType() ? out.get(ix++).asWritableObjectChunk() : null;
            final WritableLongChunk<?> timestamps = outputTimestampType() ? out.get(ix++).asWritableLongChunk() : null;
            final WritableIntChunk<?> serializedKeySize =
                    outputSerializedKeySize() ? out.get(ix++).asWritableIntChunk() : null;
            final WritableIntChunk<?> serializedValueSize =
                    outputSerializedValueSize() ? out.get(ix++).asWritableIntChunk() : null;
            for (int i = 0; i < in.size(); ++i) {
                final ConsumerRecord<?, ?> record = in.get(i);
                if (topics != null) {
                    topics.add(record.topic());
                }
                if (partitions != null) {
                    partitions.add(record.partition());
                }
                if (offsets != null) {
                    offsets.add(record.offset());
                }
                if (leaderEpochs != null) {
                    leaderEpochs.add(ConsumerRecordFunctions.leaderEpoch(record));
                }
                if (timestampTypes != null) {
                    timestampTypes.add(record.timestampType());
                }
                if (timestamps != null) {
                    timestamps.add(ConsumerRecordFunctions.timestampEpochNanos(record));
                }
                if (serializedKeySize != null) {
                    serializedKeySize.add(ConsumerRecordFunctions.serializedKeySize(record));
                }
                if (serializedValueSize != null) {
                    serializedValueSize.add(ConsumerRecordFunctions.serializedValueSize(record));
                }
            }
        }
    }
}
