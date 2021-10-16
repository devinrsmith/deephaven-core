package io.deephaven.client.examples;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import picocli.CommandLine.Option;

public class ConnectOptions {

    public static ManagedChannel open(String target, ConnectOptions options) {
        if (options == null) {
            options = new ConnectOptions();
        }
        return options.open(target);
    }

    @Option(names = {"-p", "--plaintext"}, description = "The plaintext flag.")
    Boolean plaintext;

    @Option(names = {"-u", "--user-agent"}, description = "The user-agent.")
    String userAgent;

    public ManagedChannel open(String target) {
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(target);
        if (plaintext == null) {
            if (target.startsWith("localhost:")) {
                channelBuilder.usePlaintext();
            } else {
                channelBuilder.useTransportSecurity();
            }
        } else {
            if (plaintext) {
                channelBuilder.usePlaintext();
            } else {
                channelBuilder.useTransportSecurity();
            }
        }
        if (userAgent != null) {
            channelBuilder.userAgent(userAgent);
        }
        return channelBuilder.build();
    }
}
