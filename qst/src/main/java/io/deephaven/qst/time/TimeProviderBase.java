package io.deephaven.qst.time;

import java.time.Duration;

public abstract class TimeProviderBase implements TimeProvider {

    @Override
    public final OffsetTimeProvider offset(Duration offset) {
        return ImmutableOffsetTimeProvider.builder().parent(this).offset(offset).build();
    }

    @Override
    public final OffsetTimeProvider normalized() {
        return ImmutableOffsetTimeProvider.builder().parent(this).build();
    }
}
