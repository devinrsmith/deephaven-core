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
import java.util.function.Function;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class SimpleConsumerRecordOptions {

    @Default
    @Nullable
    public String topic() {
        return "Topic";
    }

    @Default
    @Nullable
    public String partition() {
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

    /**
     *
     * @return
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

    /**
     * Creates an {@link ObjectProcessor} that contains the following output types and logic:
     *
     * <table>
     * <tr>
     * <th>If not {@code null}</th>
     * <th>Type</th>
     * <th>Logic</th>
     * </tr>
     * <tr>
     * <th>{@link #topic()}</th>
     * <th>{@link Type#stringType()}</th>
     * <th>{@link ConsumerRecord#topic()}</th>
     * </tr>
     * <tr>
     * <th>{@link #partition()}</th>
     * <th>{@link Type#intType()}</th>
     * <th>{@link ConsumerRecord#partition()}</th>
     * </tr>
     * <tr>
     * <th>{@link #offset()}</th>
     * <th>{@link Type#longType()}</th>
     * <th>{@link ConsumerRecord#offset()}</th>
     * </tr>
     * <tr>
     * <th>{@link #leaderEpoch()}</th>
     * <th>{@link Type#intType()}</th>
     * <th>{@link ConsumerRecordFunctions#leaderEpoch(ConsumerRecord)}</th>
     * </tr>
     * <tr>
     * <th>{@link #timestampType()}</th>
     * <th>{@link Type#ofCustom(Class) Type.ofCustom(TimestampType.class)}</th>
     * <th>{@link ConsumerRecord#timestampType()}</th>
     * </tr>
     * <tr>
     * <th>{@link #timestamp()}</th>
     * <th>{@link Type#instantType()}</th>
     * <th>{@link ConsumerRecordFunctions#timestampEpochNanos(ConsumerRecord)}</th>
     * </tr>
     * <tr>
     * <th>{@link #serializedKeySize()}</th>
     * <th>{@link Type#intType()}</th>
     * <th>{@link ConsumerRecordFunctions#serializedKeySize(ConsumerRecord)}</th>
     * </tr>
     * <tr>
     * <th>{@link #serializedValueSize()}</th>
     * <th>{@link Type#intType()}</th>
     * <th>{@link ConsumerRecordFunctions#serializedValueSize(ConsumerRecord)}</th>
     * </tr>
     * </table>
     *
     * @return the object processor
     */
    public final ObjectProcessor<ConsumerRecord<?, ?>> processor() {
        return new BasicConsumerRecordImpl();
    }

    public interface Builder {
        Builder topic(String topic);

        SimpleConsumerRecordOptions build();
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

    private class BasicConsumerRecordImpl implements ObjectProcessor<ConsumerRecord<?, ?>> {
        @Override
        public List<Type<?>> outputTypes() {
            return SimpleConsumerRecordOptions.this.outputTypes();
        }

        @Override
        public void processAll(ObjectChunk<? extends ConsumerRecord<?, ?>, ?> in, List<WritableChunk<?>> out) {
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
