package io.deephaven.client.examples;

import io.deephaven.client.impl.ChannelHelper;
import io.deephaven.uri.DeephavenTarget;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import picocli.CommandLine.Option;

public class ConnectOptions {

    public static final DeephavenTarget DEFAULT_TARGET = DeephavenTarget.builder()
            .host("localhost")
            .port(10000)
            .isSecure(false)
            .build();

    public static ManagedChannel open(ConnectOptions options) {
        if (options == null) {
            options = new ConnectOptions();
        }
        return options.open();
    }

    public static DeephavenTarget target(ConnectOptions options) {
        return options == null ? DEFAULT_TARGET : options.target();
    }

    @Option(names = {"-t", "--target"}, description = "The target, defaults to ${DEFAULT-VALUE}",
            defaultValue = "dh+plain://localhost:10000", converter = DeephavenTargetConverter.class)
    DeephavenTarget target;

    @Option(names = {"-u", "--user-agent"}, description = "The user-agent.")
    String userAgent;

    public DeephavenTarget target() {
        // https://github.com/remkop/picocli/issues/844
        return target == null ? DEFAULT_TARGET : target;
    }

    public ManagedChannel open() {
        final ManagedChannelBuilder<?> builder = ChannelHelper.channelBuilder(target());
        if (userAgent != null) {
            builder.userAgent(userAgent);
        }
        return builder.build();
    }
}
