package io.deephaven.server.netty;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.server.runner.Main;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class NettyMain extends Main {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {

        final NettyConfig nettyConfig = init(args, Main.class, NettyConfig.class, NettyConfig::defaultConfig);

        DaggerNettyServerComponent
                .builder()
                .withNettyConfig(nettyConfig)
                .withOut(PrintStreamGlobals.getOut())
                .withErr(PrintStreamGlobals.getErr())
                .build()
                .getServer()
                .run();
    }
}
