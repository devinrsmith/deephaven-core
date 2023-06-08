package io.deephaven.stream.blink;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.type.Type;
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

    public abstract Map<String, Mapp<T>> map();

    final TableDefinition definition() {
        return TableDefinition.from(headers());
    }

    final List<ColumnHeader<?>> headers() {
        return map()
                .entrySet()
                .stream()
                .map(e -> ColumnHeader.of(e.getKey(), e.getValue().returnType()))
                .collect(Collectors.toList());
    }

    public interface Builder<T> {

        Builder<T> name(String name);

        Builder<T> chunkSize(int chunkSize);

        Builder<T> updateSourceRegistrar(UpdateSourceRegistrar updateSourceRegistrar);

        Builder<T> putMap(String key, Mapp<T> value);

        Builder<T> putAllMap(Map<String, ? extends Mapp<T>> entries);

        default Builder<T> putInt(String key, IntMapp<T> f) {
            return putMap(key, f);
        }

        default Builder<T> putLong(String key, LongMapp<T> f) {
            return putMap(key, f);
        }

        default Builder<T> putString(String key, Function<T, String> f) {
            return putMap(key, ObjectMapp.of(f, Type.stringType()));
        }

        default Builder<T> putInstant(String key, Function<T, Instant> f) {
            return putMap(key, ObjectMapp.of(f, Type.instantType()));
        }

        BlinkTableMapperConfig<T> build();
    }
}
