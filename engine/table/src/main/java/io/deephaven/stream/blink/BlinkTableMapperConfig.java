package io.deephaven.stream.blink;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.blink.tf.BooleanFunction;
import io.deephaven.stream.blink.tf.ByteFunction;
import io.deephaven.stream.blink.tf.CharFunction;
import io.deephaven.stream.blink.tf.DoubleFunction;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.ShortFunction;
import io.deephaven.stream.blink.tf.TypedFunction;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Immutable
@BuildableStyle
public abstract class BlinkTableMapperConfig<T> {

    public static <T> BlinkTableMapperConfig.Builder<T> builder() {
        return ImmutableBlinkTableMapperConfig.builder();
    }

    public abstract String name();

    public abstract int chunkSize();

    public abstract UpdateSourceRegistrar updateSourceRegistrar();

    public abstract Map<String, TypedFunction<T>> columns();

    final TableDefinition definition() {
        return TableDefinition.from(headers());
    }

    final List<ColumnHeader<?>> headers() {
        return columns()
                .entrySet()
                .stream()
                .map(e -> ColumnHeader.of(e.getKey(), e.getValue().returnType()))
                .collect(Collectors.toList());
    }

    @Check
    final void checkColumnsNonEmpty() {
        if (columns().isEmpty()) {
            throw new IllegalArgumentException("Must be non-empty");
        }
    }

    public interface Builder<T> {

        Builder<T> name(String name);

        Builder<T> chunkSize(int chunkSize);

        Builder<T> updateSourceRegistrar(UpdateSourceRegistrar updateSourceRegistrar);

        default Builder<T> putBoolean(String key, BooleanFunction<T> f) {
            return putColumns(key, f);
        }

        default Builder<T> putByte(String key, ByteFunction<T> f) {
            return putColumns(key, f);
        }

        default Builder<T> putChar(String key, CharFunction<T> f) {
            return putColumns(key, f);
        }

        default Builder<T> putShort(String key, ShortFunction<T> f) {
            return putColumns(key, f);
        }

        default Builder<T> putInt(String key, IntFunction<T> f) {
            return putColumns(key, f);
        }

        default Builder<T> putLong(String key, LongFunction<T> f) {
            return putColumns(key, f);
        }

        default Builder<T> putFloat(String key, FloatFunction<T> f) {
            return putColumns(key, f);
        }

        default Builder<T> putDouble(String key, DoubleFunction<T> f) {
            return putColumns(key, f);
        }

        default Builder<T> putString(String key, Function<T, String> f) {
            return putColumns(key, ObjectFunction.of(f, Type.stringType()));
        }

        default Builder<T> putInstant(String key, Function<T, Instant> f) {
            return putColumns(key, ObjectFunction.of(f, Type.instantType()));
        }

        Builder<T> putColumns(String key, TypedFunction<T> value);

        Builder<T> putAllColumns(Map<String, ? extends TypedFunction<T>> entries);

        BlinkTableMapperConfig<T> build();
    }
}
