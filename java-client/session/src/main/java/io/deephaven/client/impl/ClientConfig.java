/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.uri.DeephavenTarget;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The client configuration.
 */
@Immutable
@BuildableStyle
public abstract class ClientConfig {

    public static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 100 * 1024 * 1024;

    public static Builder builder() {
        return ImmutableClientConfig.builder();
    }

    /**
     * The target.
     */
    public abstract DeephavenTarget target();

    /**
     * The SSL configuration. Only relevant if {@link #target()} is secure.
     */
    public abstract Optional<SSLConfig> ssl();

    /**
     * The user agent.
     */
    public abstract Optional<String> userAgent();

    /**
     * The extra headers.
     */
    public abstract Map<String, String> extraHeaders();

    /**
     * The maximum inbound message size. Defaults to 100MiB.
     */
    @Default
    public int maxInboundMessageSize() {
        return DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
    }

    /**
     * The combined headers, the concatenation of the {@link #target() target's} {@link DeephavenTarget#headers()
     * headers} and {@link #extraHeaders()}.
     * 
     * @return the combined headers
     */
    public final Map<String, String> combinedHeaders() {
        final Map<String, String> targetHeaders = target().headers();
        final Map<String, String> extraHeaders = extraHeaders();
        if (targetHeaders.isEmpty()) {
            return extraHeaders;
        }
        if (extraHeaders.isEmpty()) {
            return targetHeaders;
        }
        // We already know that the keys don't conflict, see #checkKeys
        final Map<String, String> combined = new LinkedHashMap<>(targetHeaders.size() + extraHeaders.size());
        combined.putAll(targetHeaders);
        combined.putAll(extraHeaders);
        return combined;
    }

    @Check
    final void checkKeys() {
        final Set<String> targetKeys = target().headers().keySet();
        for (String key : extraHeaders().keySet()) {
            if (targetKeys.contains(key)) {
                throw new IllegalArgumentException(String
                        .format("Header key'%s' specified in both the target().headers() and extraHeaders()", key));
            }
        }
    }

    public interface Builder {

        Builder target(DeephavenTarget target);

        Builder ssl(SSLConfig ssl);

        Builder userAgent(String userAgent);

        Builder putExtraHeaders(String key, String value);

        Builder putAllExtraHeaders(Map<String, ? extends String> entries);

        Builder maxInboundMessageSize(int maxInboundMessageSize);

        ClientConfig build();
    }
}
