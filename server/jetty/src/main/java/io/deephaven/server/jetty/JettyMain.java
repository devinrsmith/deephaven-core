package io.deephaven.server.jetty;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.server.runner.Main;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class JettyMain extends Main {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {

        final JettyConfig jettyConfig = init(args, Main.class, JettyConfig.class, JettyConfig::defaultConfig);

        DaggerJettyServerComponent
                .builder()
                .withJettyConfig(jettyConfig)
                .withOut(PrintStreamGlobals.getOut())
                .withErr(PrintStreamGlobals.getErr())
                .build()
                .getServer()
                .run();
    }
}
