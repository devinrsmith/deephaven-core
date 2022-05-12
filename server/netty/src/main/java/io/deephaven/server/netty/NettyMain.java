package io.deephaven.server.netty;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.runner.Main;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class NettyMain extends Main {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
        final Configuration config = init(args, Main.class);

        final String file = config.getStringWithDefault("deephaven.json", "deephaven.json");
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        final ServerConfig serverConfig = objectMapper.readValue(new File(file), ServerConfig.class);

        DaggerNettyServerComponent
                .builder()
                .withServerConfig(serverConfig)
                .withOut(PrintStreamGlobals.getOut())
                .withErr(PrintStreamGlobals.getErr())
                .build()
                .getServer()
                .run();
    }
}
