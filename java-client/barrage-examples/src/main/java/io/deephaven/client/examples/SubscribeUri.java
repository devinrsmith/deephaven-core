/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.examples;

import io.deephaven.DeephavenUri;
import io.deephaven.qst.TableCreationLogic;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "subscribe-uri", mixinStandardHelpOptions = true,
        description = "Subscribe via URI over barrage", version = "0.1.0")
class SubscribeUri extends SubscribeExampleBase {

    @Parameters(arity = "1", paramLabel = "URI", description = "URI to subscribe to.",
            converter = DeephavenUriConverter.class)
    DeephavenUri uri;

    @Override
    protected String target() {
        return uri.host().get() + ":" + uri.port().orElse(10000);
    }

    @Override
    protected TableCreationLogic logic() {
        return uri.ticket().logic();
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new SubscribeUri()).execute(args);
        System.exit(execute);
    }
}
