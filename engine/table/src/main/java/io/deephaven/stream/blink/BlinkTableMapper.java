package io.deephaven.stream.blink;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.engine.table.Table;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class BlinkTableMapper<T> {

    public static <T> BlinkTableMapper<T> create(BlinkTableMapperConfig<T> config) {
        final BlinkTableMapperImpl<T> impl = new BlinkTableMapperImpl<>(config);
        return ImmutableBlinkTableMapper.of(impl, impl.table());
    }

    @Parameter
    public abstract Producer<T> producer();

    @Parameter
    public abstract Table table();
}
