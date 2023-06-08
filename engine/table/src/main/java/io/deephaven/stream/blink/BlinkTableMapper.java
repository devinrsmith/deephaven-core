package io.deephaven.stream.blink;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.engine.table.Table;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class BlinkTableMapper<T> {

    public static <T> BlinkTableMapper<T> of(Producer<T> producer, Table table) {
        return ImmutableBlinkTableMapper.of(producer, table);
    }

    public static <T> BlinkTableMapper<T> create(BlinkTableMapperConfig<T> config) {
        final Impl<T> impl = new Impl<>(config);
        return of(impl, impl.table());
    }

    @Parameter
    public abstract Producer<T> producer();

    @Parameter
    public abstract Table table();
}
