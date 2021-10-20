package io.deephaven.client.impl;

import io.deephaven.uri.DeephavenTarget;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class ChannelHelper {

    public static ManagedChannel channel(DeephavenTarget target) {
        return channelBuilder(target).build();
    }

    public static ManagedChannelBuilder<?> channelBuilder(DeephavenTarget target) {
        // TODO: NameResolver / target
        // TODO: DNS SRV
        final int defaultPort = target.isTLS() ? Integer.getInteger("deephaven.target.port_plaintext", 8080)
                : Integer.getInteger("deephaven.target.port", 8080);
        final ManagedChannelBuilder<?> builder = ManagedChannelBuilder
                .forAddress(target.host(), target.port().orElse(defaultPort));
        if (target.isTLS()) {
            builder.useTransportSecurity();
        } else {
            builder.usePlaintext();
        }
        return builder;
    }
}
