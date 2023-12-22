/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.Function.ToLong;
import io.deephaven.json.Function.ToLong.Parser;
import io.deephaven.qst.type.Type;
import io.deephaven.time.DateTimeUtils;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class TimestampOptions extends ValueOptions {

    public enum Format {
        EPOCH_SECONDS {
            @Override
            ToLong function() {
                return ToNanos.FROM_SECONDS;
            }
        },
        EPOCH_MILLIS {
            @Override
            ToLong function() {
                return ToNanos.FROM_MILLIS;
            }
        },
        EPOCH_MICROS {
            @Override
            ToLong function() {
                return ToNanos.FROM_MICROS;
            }
        },
        EPOCH_NANOS {
            @Override
            ToLong function() {
                return Parser.LONG_VALUE;
            }
        };

        abstract ToLong function();
    }

    public static TimestampOptions of() {
        return builder().build();
    }

    public static Builder builder() {
        return null;
        // return ImmutableTimestampOptions.builder();
    }

    @Override
    @Default
    public boolean allowMissing() {
        return true;
    }

    public abstract Optional<Instant> onNull();

    public abstract Optional<Instant> onMissing();

    private long onNullOrDefault() {
        return DateTimeUtils.epochNanos(onNull().orElse(null));
    }

    private long onMissingOrDefault() {
        return DateTimeUtils.epochNanos(onMissing().orElse(null));
    }

    /**
     * The format to use, defaults to {@link Format#EPOCH_MILLIS}.
     *
     * @return the format
     */
    @Default
    public Format format() {
        return Format.EPOCH_MILLIS;
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.instantType());
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {

        return null;
        // ToLongImpl.builder()
        // .onNumberInt(format().function())
        // .build();
        //
        // return new LongChunkFromNumberIntProcessor(context, allowNull(), allowMissing(),
        // out.get(0).asWritableLongChunk(), onNullOrDefault(), onMissingOrDefault(), format().function());
    }

    // @Check
    // final void checkOnNull() {
    // if (!allowNull() && onNull().isPresent()) {
    // throw new IllegalArgumentException();
    // }
    // }
    //
    // @Check
    // final void checkOnMissing() {
    // if (!allowMissing() && onMissing().isPresent()) {
    // throw new IllegalArgumentException();
    // }
    // }

    enum ToNanos implements ToLong {
        FROM_SECONDS {
            @Override
            public long applyAsLong(JsonParser parser) throws IOException {
                // todo overflow
                return parser.getLongValue() * 1_000_000_000;
            }
        },
        FROM_MILLIS {
            @Override
            public long applyAsLong(JsonParser parser) throws IOException {
                // todo overflow
                return parser.getLongValue() * 1_000_000;
            }
        },
        FROM_MICROS {
            @Override
            public long applyAsLong(JsonParser parser) throws IOException {
                // todo overflow
                return parser.getLongValue() * 1_000;
            }
        }
    }

    public interface Builder extends ValueOptions.Builder<TimestampOptions, Builder> {
        Builder format(Format format);

        Builder onNull(Instant onNull);

        Builder onMissing(Instant onMissing);
    }
}
